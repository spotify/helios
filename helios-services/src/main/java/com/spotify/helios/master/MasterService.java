/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.master;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.servicescommon.ServiceRegistrars.createServiceRegistrar;
import static com.spotify.helios.servicescommon.ZooKeeperAclProviders.digest;
import static com.spotify.helios.servicescommon.ZooKeeperAclProviders.heliosAclProvider;

import ch.qos.logback.access.jetty.RequestLogImpl;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.AbstractIdleService;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.master.http.VersionResponseFilter;
import com.spotify.helios.master.metrics.HealthCheckGauge;
import com.spotify.helios.master.metrics.ReportingResourceMethodDispatchAdapter;
import com.spotify.helios.master.reaper.DeadAgentReaper;
import com.spotify.helios.master.reaper.ExpiredJobReaper;
import com.spotify.helios.master.reaper.JobHistoryReaper;
import com.spotify.helios.master.reaper.OldJobReaper;
import com.spotify.helios.master.resources.DeploymentGroupResource;
import com.spotify.helios.master.resources.HistoryResource;
import com.spotify.helios.master.resources.HostsResource;
import com.spotify.helios.master.resources.JobsResource;
import com.spotify.helios.master.resources.MastersResource;
import com.spotify.helios.master.resources.VersionResource;
import com.spotify.helios.rollingupdate.RollingUpdateService;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistration;
import com.spotify.helios.servicescommon.EventSender;
import com.spotify.helios.servicescommon.EventSenderFactory;
import com.spotify.helios.servicescommon.FastForwardConfig;
import com.spotify.helios.servicescommon.ManagedStatsdReporter;
import com.spotify.helios.servicescommon.ReactorFactory;
import com.spotify.helios.servicescommon.ServiceUtil;
import com.spotify.helios.servicescommon.ZooKeeperRegistrarService;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactory;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperHealthChecker;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;
import com.spotify.helios.servicescommon.statistics.FastForwardReporter;
import com.spotify.helios.servicescommon.statistics.Metrics;
import com.spotify.helios.servicescommon.statistics.MetricsImpl;
import com.spotify.helios.servicescommon.statistics.NoopMetrics;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.jetty.GzipFilterFactory;
import io.dropwizard.jetty.RequestLogFactory;
import io.dropwizard.logging.AppenderFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.setup.Environment;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.ACL;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Helios master service.
 */
public class MasterService extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(MasterService.class);

  private static final String LOGBACK_ACCESS_CONFIG = "logback-access.xml";
  private static final String LOGBACK_ACCESS_RESOURCE = "/" + LOGBACK_ACCESS_CONFIG;

  private final Server server;
  private final MasterConfig config;
  private final ServiceRegistrar registrar;
  private final ZooKeeperClient zooKeeperClient;
  private final ExpiredJobReaper expiredJobReaper;
  private final CuratorClientFactory curatorClientFactory;
  private final RollingUpdateService rollingUpdateService;
  private final Optional<DeadAgentReaper> agentReaper;
  private final Optional<OldJobReaper> oldJobReaper;
  private final Optional<JobHistoryReaper> jobHistoryReaper;

  private ZooKeeperRegistrarService zkRegistrar;

  /**
   * Create a new service instance. Initializes the control interface and the worker.
   *
   * @param config               The service configuration.
   * @param environment          The DropWizard environment.
   * @param curatorClientFactory The zookeeper curator factory.
   *
   * @throws ConfigurationException If there is a problem with the DropWizard configuration.
   * @throws IOException            IOException
   * @throws InterruptedException   InterruptedException
   */
  public MasterService(final MasterConfig config,
                       final Environment environment,
                       final CuratorClientFactory curatorClientFactory)
      throws ConfigurationException, IOException, InterruptedException {
    this.config = config;
    this.curatorClientFactory = curatorClientFactory;

    // Configure metrics
    final MetricRegistry metricsRegistry = environment.metrics();
    metricsRegistry.registerAll(new GarbageCollectorMetricSet());
    metricsRegistry.registerAll(new MemoryUsageGaugeSet());

    log.info("Starting metrics");
    final Metrics metrics;
    if (config.isInhibitMetrics()) {
      metrics = new NoopMetrics();
    } else {
      metrics = new MetricsImpl(metricsRegistry, MetricsImpl.Type.MASTER);
      metrics.start();
      if (!Strings.isNullOrEmpty(config.getStatsdHostPort())) {
        environment.lifecycle().manage(new ManagedStatsdReporter(config.getStatsdHostPort(),
            metricsRegistry));
      }

      final FastForwardConfig ffwdConfig = config.getFfwdConfig();
      if (ffwdConfig != null) {
        environment.lifecycle().manage(FastForwardReporter.create(
            metricsRegistry,
            ffwdConfig.getAddress(),
            ffwdConfig.getMetricKey(),
            ffwdConfig.getReportingIntervalSeconds())
        );
      }
    }

    // Set up the master model
    this.zooKeeperClient = setupZookeeperClient(config);
    final ZooKeeperModelReporter modelReporter =
        new ZooKeeperModelReporter(metrics.getZooKeeperMetrics());
    final ZooKeeperClientProvider zkClientProvider = new ZooKeeperClientProvider(
        zooKeeperClient, modelReporter);

    // Create state directory, if necessary
    final Path stateDirectory = config.getStateDirectory().toAbsolutePath().normalize();
    if (!Files.exists(stateDirectory)) {
      try {
        Files.createDirectories(stateDirectory);
      } catch (IOException e) {
        log.error("Failed to create state directory: {}", stateDirectory, e);
        throw new RuntimeException(e);
      }
    }

    final String deploymentGroupEventTopic = TaskStatusEvent.TASK_STATUS_EVENT_TOPIC;

    final List<EventSender> eventSenders =
        EventSenderFactory.build(environment, config, metricsRegistry, deploymentGroupEventTopic);

    final ZooKeeperMasterModel model =
        new ZooKeeperMasterModel(zkClientProvider, config.getName(), eventSenders,
            deploymentGroupEventTopic);

    final ZooKeeperHealthChecker zooKeeperHealthChecker =
        new ZooKeeperHealthChecker(zooKeeperClient);

    environment.healthChecks().register("zookeeper", zooKeeperHealthChecker);

    // Report health checks as a gauge metric
    environment.healthChecks().getNames().forEach(
        name -> environment.metrics().register(
            "helios." + name + ".ok", new HealthCheckGauge(environment.healthChecks(), name)));

    // Set up service registrar
    this.registrar = createServiceRegistrar(config.getServiceRegistrarPlugin(),
        config.getServiceRegistryAddress(),
        config.getDomain());

    // Set up reaping of expired jobs
    this.expiredJobReaper = ExpiredJobReaper.newBuilder()
        .setMasterModel(model)
        .build();

    // Set up rolling update service
    final ReactorFactory reactorFactory = new ReactorFactory();
    this.rollingUpdateService = new RollingUpdateService(model, reactorFactory);

    // Set up agent reaper (de-registering hosts that have been DOWN for more than X hours)
    if (config.getAgentReapingTimeout() > 0) {
      this.agentReaper = Optional.of(new DeadAgentReaper(model, config.getAgentReapingTimeout()));
    } else {
      log.info("Reaping of dead agents disabled");
      this.agentReaper = Optional.empty();
    }

    // Set up old job reaper (removes jobs not deployed anywhere and created more than X days ago)
    if (config.getJobRetention() > 0) {
      this.oldJobReaper = Optional.of(new OldJobReaper(model, config.getJobRetention()));
    } else {
      log.info("Reaping of old jobs disabled");
      this.oldJobReaper = Optional.empty();
    }

    // Set up job history reaper (removes histories whose corresponding job doesn't exist)
    if (config.isJobHistoryReapingEnabled()) {
      this.jobHistoryReaper = Optional.of(
          new JobHistoryReaper(model, zkClientProvider.get("jobHistoryReaper")));
    } else {
      log.info("Reaping of orphaned jobs disabled");
      this.jobHistoryReaper = Optional.empty();
    }

    // Set up http server
    environment.servlets()
        .addFilter("VersionResponseFilter", new VersionResponseFilter(metrics.getMasterMetrics()))
        .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");
    environment.jersey().register(
        new ReportingResourceMethodDispatchAdapter(metrics.getMasterMetrics()));
    environment.jersey().register(new JobsResource(
        model, metrics.getMasterMetrics(), config.getWhitelistedCapabilities()));
    environment.jersey().register(new HistoryResource(model, metrics.getMasterMetrics()));
    environment.jersey().register(new HostsResource(model));
    environment.jersey().register(new MastersResource(model));
    environment.jersey().register(new VersionResource());
    environment.jersey().register(new UserProvider());
    environment.jersey().register(new DeploymentGroupResource(model));

    final DefaultServerFactory serverFactory = ServiceUtil.createServerFactory(
        config.getHttpEndpoint(), config.getAdminEndpoint(), false);

    final RequestLogFactory requestLog = new RequestLogFactory();
    requestLog.setAppenders(ImmutableList.<AppenderFactory>of());
    serverFactory.setRequestLogFactory(requestLog);

    // Enable CORS headers
    final FilterRegistration.Dynamic cors = environment.servlets()
        .addFilter("CORS", CrossOriginFilter.class);

    // Configure CORS parameters
    cors.setInitParameter("allowedOrigins", "*");
    cors.setInitParameter("allowedHeaders", "X-Requested-With,Content-Type,Accept,Origin");
    cors.setInitParameter("allowedMethods", "OPTIONS,GET,PUT,POST,DELETE,HEAD");

    // Add URL mapping
    cors.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

    // Enable gzip compression for POST and GET requests. Default is GET only.
    final GzipFilterFactory gzip = new GzipFilterFactory();
    gzip.setIncludedMethods(ImmutableSet.of("GET", "POST"));
    serverFactory.setGzipFilterFactory(gzip);

    this.server = serverFactory.build(environment);

    setUpRequestLogging(stateDirectory);
  }

  private void setUpRequestLogging(final Path stateDirectory) {
    // Set up request logging
    final Handler originalHandler = server.getHandler();
    final HandlerCollection handlerCollection;
    if (originalHandler instanceof HandlerCollection) {
      handlerCollection = (HandlerCollection) originalHandler;
    } else {
      handlerCollection = new HandlerCollection();
      handlerCollection.addHandler(originalHandler);
    }

    final RequestLogHandler requestLogHandler = new RequestLogHandler();
    final RequestLogImpl requestLog = new RequestLogImpl();
    requestLog.setQuiet(true);

    if (stateDirectory.resolve(LOGBACK_ACCESS_CONFIG).toFile().exists()) {
      requestLog.setFileName(stateDirectory.resolve(LOGBACK_ACCESS_CONFIG).toString());
    } else if (this.getClass().getResource(LOGBACK_ACCESS_RESOURCE) != null) {
      requestLog.setResource(LOGBACK_ACCESS_RESOURCE);
    }

    requestLogHandler.setRequestLog(requestLog);
    handlerCollection.addHandler(requestLogHandler);
    server.setHandler(handlerCollection);
  }

  @Override
  protected void startUp() throws Exception {
    logBanner();
    if (!config.getNoZooKeeperMasterRegistration()) {
      zkRegistrar.startAsync().awaitRunning();
    }
    expiredJobReaper.startAsync().awaitRunning();
    rollingUpdateService.startAsync().awaitRunning();

    agentReaper.ifPresent(reaper -> reaper.startAsync().awaitRunning());
    oldJobReaper.ifPresent(reaper -> reaper.startAsync().awaitRunning());
    jobHistoryReaper.ifPresent(reaper -> reaper.startAsync().awaitRunning());

    try {
      server.start();
    } catch (Exception e) {
      log.error("Unable to start server, shutting down", e);
      server.stop();
    }

    final ServiceRegistration serviceRegistration = ServiceRegistration.newBuilder()
        .endpoint("helios", "http", config.getHttpEndpoint().getPort(),
            config.getDomain(), config.getName())
        .build();
    registrar.register(serviceRegistration);
  }

  @Override
  protected void shutDown() throws Exception {
    server.stop();
    server.join();
    registrar.close();

    agentReaper.ifPresent(reaper -> reaper.stopAsync().awaitTerminated());
    oldJobReaper.ifPresent(reaper -> reaper.stopAsync().awaitTerminated());
    jobHistoryReaper.ifPresent(reaper -> reaper.stopAsync().awaitTerminated());

    rollingUpdateService.stopAsync().awaitTerminated();
    expiredJobReaper.stopAsync().awaitTerminated();
    zkRegistrar.stopAsync().awaitTerminated();
    zooKeeperClient.close();
  }

  private void logBanner() {
    try {
      final String banner = Resources.toString(Resources.getResource("master-banner.txt"), UTF_8);
      log.info("\n{}", banner);
    } catch (IllegalArgumentException | IOException ignored) {
      // ignored
    }
  }

  /**
   * Create a Zookeeper client and create the control and state nodes if needed.
   *
   * @param config The service configuration.
   *
   * @return A zookeeper client.
   */
  private ZooKeeperClient setupZookeeperClient(final MasterConfig config) {
    ACLProvider aclProvider = null;
    List<AuthInfo> authorization = null;

    final String masterUser = config.getZookeeperAclMasterUser();
    final String masterPassword = config.getZooKeeperAclMasterPassword();
    final String agentUser = config.getZookeeperAclAgentUser();
    final String agentDigest = config.getZooKeeperAclAgentDigest();

    if (!isNullOrEmpty(masterPassword)) {
      if (isNullOrEmpty(masterUser)) {
        throw new HeliosRuntimeException(
            "Master username must be set if a password is set");
      }

      authorization = Lists.newArrayList(new AuthInfo(
          "digest", String.format("%s:%s", masterUser, masterPassword).getBytes()));
    }

    if (config.isZooKeeperEnableAcls()) {
      if (isNullOrEmpty(masterUser) || isNullOrEmpty(masterPassword)) {
        throw new HeliosRuntimeException(
            "ZooKeeper ACLs enabled but master username and/or password not set");
      }

      if (isNullOrEmpty(agentUser) || isNullOrEmpty(agentDigest)) {
        throw new HeliosRuntimeException(
            "ZooKeeper ACLs enabled but agent username and/or digest not set");
      }

      aclProvider = heliosAclProvider(
          masterUser, digest(masterUser, masterPassword),
          agentUser, agentDigest);
    }

    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = curatorClientFactory.newClient(
        config.getZooKeeperConnectionString(),
        config.getZooKeeperSessionTimeoutMillis(),
        config.getZooKeeperConnectionTimeoutMillis(),
        zooKeeperRetryPolicy,
        aclProvider,
        authorization);
    final ZooKeeperClient client =
        new DefaultZooKeeperClient(curator, config.getZooKeeperClusterId());
    client.start();
    zkRegistrar = ZooKeeperRegistrarService.newBuilder()
        .setZooKeeperClient(client)
        .setZooKeeperRegistrar(new MasterZooKeeperRegistrar(config.getName()))
        .build();

    // TODO: This is perhaps not the correct place to do this - but at present it's the only
    // place where we have access to the ACL provider.
    if (aclProvider != null) {
      // Set ACLs on the ZK root, if they aren't already set correctly.
      // This is handy since it avoids having to manually do this operation when setting up
      // a new ZK cluster.
      // Note that this is slightly racey -- if two masters start at the same time both might
      // attempt to update the ACLs but only one will succeed. That said, it's unlikely and the
      // effects are limited to a spurious log line.
      try {
        final List<ACL> curAcls = client.getAcl("/");
        final List<ACL> wantedAcls = aclProvider.getAclForPath("/");
        if (!Sets.newHashSet(curAcls).equals(Sets.newHashSet(wantedAcls))) {
          log.info(
              "Current ACL's on the zookeeper root node differ from desired, updating: {} -> {}",
              curAcls, wantedAcls);
          client.getCuratorFramework().setACL().withACL(wantedAcls).forPath("/");
        }
      } catch (Exception e) {
        log.error("Failed to get/set ACLs on the zookeeper root node", e);
      }
    }

    return client;
  }
}

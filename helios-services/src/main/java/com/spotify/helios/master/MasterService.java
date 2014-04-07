/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.google.common.io.Resources;
import com.google.common.util.concurrent.AbstractIdleService;

import com.spotify.helios.master.http.VersionResponseFilter;
import com.spotify.helios.master.metrics.ReportingResourceMethodDispatchAdapter;
import com.spotify.helios.master.resources.HostsResource;
import com.spotify.helios.master.resources.JobsResource;
import com.spotify.helios.master.resources.MastersResource;
import com.spotify.helios.master.resources.VersionResource;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistration;
import com.spotify.helios.servicescommon.ManagedStatsdReporter;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.RiemannHeartBeat;
import com.spotify.helios.servicescommon.RiemannSupport;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperHealthChecker;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;
import com.spotify.helios.servicescommon.statistics.Metrics;
import com.spotify.helios.servicescommon.statistics.MetricsImpl;
import com.spotify.helios.servicescommon.statistics.NoopMetrics;
import com.yammer.dropwizard.config.ConfigurationException;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.config.RequestLogConfiguration;
import com.yammer.dropwizard.config.ServerFactory;
import com.yammer.dropwizard.lifecycle.ServerLifecycleListener;
import com.yammer.metrics.core.MetricsRegistry;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.access.jetty.RequestLogImpl;

import static com.google.common.base.Charsets.UTF_8;
import static com.spotify.helios.servicescommon.ServiceRegistrars.createServiceRegistrar;

/**
 * The Helios master service.
 */
public class MasterService extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(MasterService.class);

  private final Server server;
  private final MasterConfig config;
  private final Environment environment;
  private final ServiceRegistrar registrar;
  private final RiemannFacade riemannFacade;
  private final ZooKeeperClient zooKeeperClient;

  private PersistentEphemeralNode upNode;

  /**
   * Create a new service instance. Initializes the control interface and the worker.
   *
   * @param config The service configuration.
   */
  public MasterService(final MasterConfig config, final Environment environment)
      throws ConfigurationException {
    this.config = config;
    this.environment = environment;

    // Configure metrics
    // TODO (dano): do something with the riemann facade
    final MetricsRegistry metricsRegistry = com.yammer.metrics.Metrics.defaultRegistry();
    RiemannSupport riemannSupport = new RiemannSupport(metricsRegistry, config.getRiemannHostPort(),
        config.getName(), "helios-master");
    riemannFacade = riemannSupport.getFacade();
    log.info("Starting metrics");
    final Metrics metrics;
    if (config.isInhibitMetrics()) {
      metrics = new NoopMetrics();
    } else {
      metrics = new MetricsImpl(metricsRegistry);
      metrics.start();
      environment.manage(riemannSupport);
      environment.manage(new ManagedStatsdReporter(config.getStatsdHostPort(), "helios-master",
          metricsRegistry));
    }

    // Set up the master model
    this.zooKeeperClient = setupZookeeperClient(config);
    final MasterModel model = new ZooKeeperMasterModel(
        new ZooKeeperClientProvider(zooKeeperClient,
            new ZooKeeperModelReporter(riemannFacade, metrics.getZooKeeperMetrics())));
    ZooKeeperHealthChecker zooKeeperHealthChecker = new ZooKeeperHealthChecker(zooKeeperClient,
        Paths.statusMasters(), riemannFacade, TimeUnit.MINUTES, 2);
    environment.manage(zooKeeperHealthChecker);
    environment.addHealthCheck(zooKeeperHealthChecker);
    environment.manage(new RiemannHeartBeat(TimeUnit.MINUTES, 2, riemannFacade));

    // Set up service registrar
    this.registrar = createServiceRegistrar(config.getServiceRegistrarPlugin(),
                                            config.getServiceRegistryAddress(),
                                            config.getDomain());

    // Set up http server
    environment.addFilter(VersionResponseFilter.class, "/*");
    environment.addProvider(new ReportingResourceMethodDispatchAdapter(metrics.getMasterMetrics()));
    environment.addResource(new JobsResource(model, metrics.getMasterMetrics()));
    environment.addResource(new HistoryResource(model));
    environment.addResource(new HostsResource(model));
    environment.addResource(new MastersResource(model));
    environment.addResource(new VersionResource());
    final RequestLogConfiguration requestLogConfiguration =
        config.getHttpConfiguration().getRequestLogConfiguration();
    requestLogConfiguration.getConsoleConfiguration().setEnabled(false);
    requestLogConfiguration.getSyslogConfiguration().setEnabled(false);
    requestLogConfiguration.getFileConfiguration().setEnabled(false);
    this.server = new ServerFactory(config.getHttpConfiguration(), environment.getName())
        .buildServer(environment);

    // Set up request logging
    final HandlerCollection handler = (HandlerCollection) server.getHandler();
    final RequestLogHandler requestLogHandler = new RequestLogHandler();
    final RequestLogImpl requestLog = new RequestLogImpl();
    requestLog.setQuiet(true);
    requestLog.setResource("/logback-access.xml");
    requestLogHandler.setRequestLog(requestLog);
    handler.addHandler(requestLogHandler);
    server.setHandler(handler);
  }

  @Override
  protected void startUp() throws Exception {
    logBanner();
    try {
      server.start();
      for (ServerLifecycleListener listener : environment.getServerListeners()) {
        listener.serverStarted(server);
      }
    } catch (Exception e) {
      log.error("Unable to start server, shutting down", e);
      server.stop();
    }

    final ServiceRegistration serviceRegistration = ServiceRegistration.newBuilder()
        .endpoint("helios", "http", config.getHttpConfiguration().getPort())
        .build();
    registrar.register(serviceRegistration);
  }

  @Override
  protected void shutDown() throws Exception {
    server.stop();
    server.join();
    registrar.close();
    if (upNode != null) {
      try {
        upNode.close();
      } catch (IOException e) {
        log.warn("Exception on closing up node: {}", e.getMessage());
      }
    }
    zooKeeperClient.close();
  }

  private void logBanner() {
    try {
      final String banner = Resources.toString(Resources.getResource("banner.txt"), UTF_8);
      log.info("\n{}", banner);
    } catch (IllegalArgumentException | IOException ignored) {
    }
  }

  /**
   * Create a Zookeeper client and create the control and state nodes if needed.
   *
   * @param config The service configuration.
   * @return A zookeeper client.
   */
  private ZooKeeperClient setupZookeeperClient(final MasterConfig config) {
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = CuratorFrameworkFactory.newClient(
        config.getZooKeeperConnectString(),
        config.getZooKeeperSessionTimeoutMillis(),
        config.getZooKeeperConnectionTimeoutMillis(),
        zooKeeperRetryPolicy);
    final ZooKeeperClient client = new DefaultZooKeeperClient(curator);
    client.start();

    // TODO (dano): move directory initialization elsewhere
    try {
      client.ensurePath(Paths.configHosts());
      client.ensurePath(Paths.configJobs());
      client.ensurePath(Paths.configJobRefs());
      client.ensurePath(Paths.statusHosts());
      client.ensurePath(Paths.statusMasters());
      client.ensurePath(Paths.historyJobs());

      final String upPath = Paths.statusMasterUp(config.getName());
      upNode = client.persistentEphemeralNode(upPath, PersistentEphemeralNode.Mode.EPHEMERAL, new byte[]{});
      upNode.start();
    } catch (KeeperException e) {
      throw new RuntimeException("zookeeper initialization failed", e);
    }

    return client;
  }
}

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

package com.spotify.helios.agent;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.agent.Agent.EMPTY_EXECUTIONS;
import static com.spotify.helios.servicescommon.ServiceRegistrars.createServiceRegistrar;
import static com.spotify.helios.servicescommon.ZooKeeperAclProviders.digest;
import static com.spotify.helios.servicescommon.ZooKeeperAclProviders.heliosAclProvider;
import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.AbstractIdleService;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.gcr.ContainerRegistryAuthSupplier;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.SystemClock;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.master.metrics.HealthCheckGauge;
import com.spotify.helios.master.metrics.TotalHealthCheckGauge;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.EventSender;
import com.spotify.helios.servicescommon.EventSenderFactory;
import com.spotify.helios.servicescommon.FastForwardConfig;
import com.spotify.helios.servicescommon.ManagedStatsdReporter;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.ReactorFactory;
import com.spotify.helios.servicescommon.ServiceUtil;
import com.spotify.helios.servicescommon.ZooKeeperRegistrarService;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactoryImpl;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperHealthChecker;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;
import com.spotify.helios.servicescommon.coordination.ZooKeeperNodeUpdaterFactory;
import com.spotify.helios.servicescommon.statistics.DockerVersionSupplier;
import com.spotify.helios.servicescommon.statistics.FastForwardReporter;
import com.spotify.helios.servicescommon.statistics.Metrics;
import com.spotify.helios.servicescommon.statistics.MetricsImpl;
import com.spotify.helios.servicescommon.statistics.NoopMetrics;
import com.sun.management.OperatingSystemMXBean;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Helios agent.
 */
public class AgentService extends AbstractIdleService implements Managed {

  private static final Logger log = LoggerFactory.getLogger(AgentService.class);

  private static final String TASK_HISTORY_FILENAME = "task-history.json";
  private static final TypeReference<Map<JobId, Execution>> JOBID_EXECUTIONS_MAP =
      new TypeReference<Map<JobId, Execution>>() {
      };

  private final Agent agent;

  private final Server server;
  private final ZooKeeperClient zooKeeperClient;
  private final HostInfoReporter hostInfoReporter;
  private final AgentInfoReporter agentInfoReporter;
  private final EnvironmentVariableReporter environmentVariableReporter;
  private final LabelReporter labelReporter;
  private final FileChannel stateLockFile;
  private final FileLock stateLock;
  private final ZooKeeperAgentModel model;
  private final Metrics metrics;
  private final ServiceRegistrar serviceRegistrar;

  private ZooKeeperRegistrarService zkRegistrar;

  /**
   * Create a new agent instance.
   *
   * @param config      The service configuration.
   * @param environment The DropWizard environment.
   *
   * @throws ConfigurationException If an error occurs with the DropWizard configuration.
   * @throws InterruptedException   If the thread is interrupted.
   * @throws IOException            IOException
   */
  public AgentService(final AgentConfig config, final Environment environment)
      throws ConfigurationException, InterruptedException, IOException {
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

    // Take a file lock in the state directory to ensure this is the only agent using it
    final Path lockPath = config.getStateDirectory().resolve("lock");
    try {
      stateLockFile = FileChannel.open(lockPath, CREATE, WRITE);
      stateLock = stateLockFile.tryLock();
      if (stateLock == null) {
        throw new IllegalStateException("State lock file already locked: " + lockPath);
      }
    } catch (OverlappingFileLockException e) {
      throw new IllegalStateException("State lock file already locked: " + lockPath);
    } catch (IOException e) {
      log.error("Failed to take state lock: {}", lockPath, e);
      throw new RuntimeException(e);
    }

    final Path idPath = config.getStateDirectory().resolve("id");
    final String id;
    try {
      if (Files.exists(idPath)) {
        id = new String(Files.readAllBytes(idPath), UTF_8);
      } else {
        id = config.getId();
        Files.write(idPath, id.getBytes(UTF_8));
      }
    } catch (IOException e) {
      log.error("Failed to set up id file: {}", idPath, e);
      throw new RuntimeException(e);
    }

    // Configure metrics
    final MetricRegistry metricsRegistry = environment.metrics();
    metricsRegistry.registerAll(new GarbageCollectorMetricSet());
    metricsRegistry.registerAll(new MemoryUsageGaugeSet());

    final DockerClient dockerClient = createDockerClient(config);

    if (config.isInhibitMetrics()) {
      log.info("Not starting metrics");
      metrics = new NoopMetrics();
    } else {
      log.info("Starting metrics");
      metrics = new MetricsImpl(metricsRegistry, MetricsImpl.Type.AGENT);

      if (!Strings.isNullOrEmpty(config.getStatsdHostPort())) {
        environment.lifecycle().manage(new ManagedStatsdReporter(config.getStatsdHostPort(),
            metricsRegistry));
      }

      final FastForwardConfig ffwdConfig = config.getFfwdConfig();
      if (ffwdConfig != null) {

        // include the version docker as an additional attribute in FastForwardReporter
        final DockerVersionSupplier versionSupplier = new DockerVersionSupplier(dockerClient);

        final Supplier<Map<String, String>> attributesSupplier =
            () -> ImmutableMap.of("docker_version", versionSupplier.get());

        final FastForwardReporter reporter = FastForwardReporter.create(
            metricsRegistry,
            ffwdConfig.getAddress(),
            ffwdConfig.getMetricKey(),
            ffwdConfig.getReportingIntervalSeconds(),
            attributesSupplier);

        environment.lifecycle().manage(reporter);
      }
    }

    // This CountDownLatch will signal EnvironmentVariableReporter and LabelReporter when to report
    // data to ZK. They only report once and then stop, so we need to tell them when to start
    // reporting otherwise they'll race with ZooKeeperRegistrarService and might have their data
    // erased if they are too fast.
    final CountDownLatch zkRegistrationSignal = new CountDownLatch(1);

    this.zooKeeperClient = setupZookeeperClient(config, id, zkRegistrationSignal);
    final DockerHealthChecker dockerHealthChecker = new DockerHealthChecker(
        metrics.getSupervisorMetrics(), TimeUnit.SECONDS, 30);
    environment.lifecycle().manage(dockerHealthChecker);

    // Set up model
    final ZooKeeperModelReporter modelReporter =
        new ZooKeeperModelReporter(metrics.getZooKeeperMetrics());
    final ZooKeeperClientProvider zkClientProvider = new ZooKeeperClientProvider(
        zooKeeperClient, modelReporter);

    final String taskStatusEventTopic = TaskStatusEvent.TASK_STATUS_EVENT_TOPIC;

    final List<EventSender> eventSenders = EventSenderFactory
        .build(environment, config, metricsRegistry, taskStatusEventTopic);

    final TaskHistoryWriter historyWriter;
    if (config.isJobHistoryDisabled()) {
      historyWriter = null;
    } else {
      historyWriter = new TaskHistoryWriter(
          config.getName(), zooKeeperClient, stateDirectory.resolve(TASK_HISTORY_FILENAME));
    }

    try {
      this.model =
          new ZooKeeperAgentModel(zkClientProvider, config.getName(), stateDirectory, historyWriter,
              eventSenders, taskStatusEventTopic);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Set up service registrar
    this.serviceRegistrar = createServiceRegistrar(config.getServiceRegistrarPlugin(),
        config.getServiceRegistryAddress(),
        config.getDomain());

    final ZooKeeperNodeUpdaterFactory nodeUpdaterFactory =
        new ZooKeeperNodeUpdaterFactory(zooKeeperClient);

    this.hostInfoReporter =
        new HostInfoReporter((OperatingSystemMXBean) getOperatingSystemMXBean(), nodeUpdaterFactory,
            config.getName(), dockerClient, config.getDockerHost(),
            1, TimeUnit.MINUTES, zkRegistrationSignal);

    this.agentInfoReporter =
        new AgentInfoReporter(getRuntimeMXBean(), nodeUpdaterFactory, config.getName(),
            1, TimeUnit.MINUTES, zkRegistrationSignal);

    this.environmentVariableReporter = new EnvironmentVariableReporter(
        config.getName(), config.getEnvVars(), nodeUpdaterFactory, zkRegistrationSignal);

    this.labelReporter = new LabelReporter(
        config.getName(), config.getLabels(), nodeUpdaterFactory, zkRegistrationSignal);

    final String namespace = "helios-" + id;

    final List<ContainerDecorator> decorators = Lists.newArrayList();

    if (!isNullOrEmpty(config.getRedirectToSyslog())) {
      decorators.add(new SyslogRedirectingContainerDecorator(config.getRedirectToSyslog()));
    }

    if (!config.getBinds().isEmpty()) {
      decorators.add(new BindVolumeContainerDecorator(config.getBinds()));
    }

    if (!config.getExtraHosts().isEmpty()) {
      decorators.add(new AddExtraHostContainerDecorator(config.getExtraHosts()));
    }

    final SupervisorFactory supervisorFactory = new SupervisorFactory(
        model, dockerClient,
        config.getEnvVars(), serviceRegistrar,
        decorators,
        config.getDockerHost(),
        config.getName(),
        metrics.getSupervisorMetrics(),
        namespace,
        config.getDomain(),
        config.getDns());

    final ReactorFactory reactorFactory = new ReactorFactory();

    final PortAllocator portAllocator = new PortAllocator(config.getPortRangeStart(),
        config.getPortRangeEnd());

    final PersistentAtomicReference<Map<JobId, Execution>> executions;
    try {
      executions = PersistentAtomicReference.create(stateDirectory.resolve("executions.json"),
          JOBID_EXECUTIONS_MAP,
          Suppliers.ofInstance(EMPTY_EXECUTIONS));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final Reaper reaper = new Reaper(dockerClient, namespace);
    this.agent = new Agent(model, supervisorFactory, reactorFactory, executions, portAllocator,
        reaper);

    final ZooKeeperHealthChecker zkHealthChecker = new ZooKeeperHealthChecker(zooKeeperClient);
    final DockerDaemonHealthChecker dockerDaemonHealthChecker =
        new DockerDaemonHealthChecker(dockerClient);

    if (!config.getNoHttp()) {

      environment.healthChecks().register("docker", dockerHealthChecker);
      environment.healthChecks().register("zookeeper", zkHealthChecker);
      environment.healthChecks().register("dockerd", dockerDaemonHealthChecker);

      // Report each individual healthcheck as a gauge metric
      environment.healthChecks().getNames().forEach(
          name -> environment.metrics().register(
              "helios." + name + ".ok", new HealthCheckGauge(environment.healthChecks(), name)));

      // and add one gauge for the overall health, similar to what HealthCheckServlet does - if
      // any healthcheck fails, then report overall health of false.
      // this causes each healthcheck to be executed twice each time metrics are reported, but
      // this feels ok since each check is cheap.
      environment.metrics().register("helios.healthy",
          new TotalHealthCheckGauge(environment.healthChecks()));

      environment.jersey().register(new AgentModelTaskResource(model));
      environment.jersey().register(new AgentModelTaskStatusResource(model));
      environment.lifecycle().manage(this);

      this.server = ServiceUtil.createServerFactory(config.getHttpEndpoint(),
          config.getAdminEndpoint(),
          config.getNoHttp())
          .build(environment);
    } else {
      this.server = null;
    }
    environment.lifecycle().manage(this);
  }

  private DockerClient createDockerClient(final AgentConfig config) throws IOException {
    final DefaultDockerClient.Builder builder = DefaultDockerClient.builder()
        .uri(config.getDockerHost().uri());

    if (config.getConnectionPoolSize() != -1) {
      builder.connectionPoolSize(config.getConnectionPoolSize());
    }

    if (!isNullOrEmpty(config.getDockerHost().dockerCertPath())) {
      final Path dockerCertPath = java.nio.file.Paths.get(config.getDockerHost().dockerCertPath());
      final DockerCertificates dockerCertificates;
      try {
        dockerCertificates = new DockerCertificates(dockerCertPath);
      } catch (DockerCertificateException e) {
        throw new RuntimeException(e);
      }

      builder.dockerCertificates(dockerCertificates);
    }

    if (config.getGoogleCredentials() != null) {
      builder.registryAuthSupplier(
          ContainerRegistryAuthSupplier
              .forCredentials(config.getGoogleCredentials())
              .build()
      );
    }
    return new PollingDockerClient(builder);
  }

  /**
   * Create a Zookeeper client and create the control and state nodes if needed.
   *
   * @param config The service configuration.
   *
   * @return A zookeeper client.
   */
  private ZooKeeperClient setupZookeeperClient(final AgentConfig config, final String id,
                                               final CountDownLatch zkRegistrationSignal) {
    ACLProvider aclProvider = null;
    List<AuthInfo> authorization = null;

    final String agentUser = config.getZookeeperAclAgentUser();
    final String agentPassword = config.getZooKeeperAclAgentPassword();
    final String masterUser = config.getZookeeperAclMasterUser();
    final String masterDigest = config.getZooKeeperAclMasterDigest();

    if (!isNullOrEmpty(agentPassword)) {
      if (isNullOrEmpty(agentUser)) {
        throw new HeliosRuntimeException(
            "Agent username must be set if a password is set");
      }

      authorization = Lists.newArrayList(new AuthInfo(
          "digest", String.format("%s:%s", agentUser, agentPassword).getBytes()));
    }

    if (config.isZooKeeperEnableAcls()) {
      if (isNullOrEmpty(agentUser) || isNullOrEmpty(agentPassword)) {
        throw new HeliosRuntimeException(
            "ZooKeeper ACLs enabled but agent username and/or password not set");
      }

      if (isNullOrEmpty(masterUser) || isNullOrEmpty(masterDigest)) {
        throw new HeliosRuntimeException(
            "ZooKeeper ACLs enabled but master username and/or digest not set");
      }

      aclProvider = heliosAclProvider(
          masterUser, masterDigest,
          agentUser, digest(agentUser, agentPassword));
    }

    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = new CuratorClientFactoryImpl().newClient(
        config.getZooKeeperConnectionString(),
        config.getZooKeeperSessionTimeoutMillis(),
        config.getZooKeeperConnectionTimeoutMillis(),
        zooKeeperRetryPolicy,
        aclProvider,
        authorization);

    final ZooKeeperClient client = new DefaultZooKeeperClient(curator,
        config.getZooKeeperClusterId());
    client.start();

    // Register the agent
    final AgentZooKeeperRegistrar agentZooKeeperRegistrar = new AgentZooKeeperRegistrar(
        config.getName(), id, config.getZooKeeperRegistrationTtlMinutes(), new SystemClock());
    zkRegistrar = ZooKeeperRegistrarService.newBuilder()
        .setZooKeeperClient(client)
        .setZooKeeperRegistrar(agentZooKeeperRegistrar)
        .setZkRegistrationSignal(zkRegistrationSignal)
        .build();

    return client;
  }

  @Override
  protected void startUp() throws Exception {
    logBanner();
    zkRegistrar.startAsync().awaitRunning();
    model.startAsync().awaitRunning();
    agent.startAsync().awaitRunning();
    hostInfoReporter.startAsync();
    agentInfoReporter.startAsync();
    environmentVariableReporter.startAsync();
    labelReporter.startAsync();
    metrics.start();
    if (server != null) {
      try {
        server.start();
      } catch (Exception e) {
        log.error("Unable to start server, shutting down", e);
        server.stop();
      }
    }
  }

  private void logBanner() {
    try {
      final String banner = Resources.toString(Resources.getResource("agent-banner.txt"), UTF_8);
      log.info("\n{}", banner);
    } catch (IllegalArgumentException | IOException ignored) {
      // ignore
    }
  }

  @Override
  protected void shutDown() throws Exception {
    if (server != null) {
      server.stop();
    }
    hostInfoReporter.stopAsync().awaitTerminated();
    agentInfoReporter.stopAsync().awaitTerminated();
    environmentVariableReporter.stopAsync().awaitTerminated();
    labelReporter.stopAsync().awaitTerminated();
    agent.stopAsync().awaitTerminated();

    if (serviceRegistrar != null) {
      serviceRegistrar.close();
    }
    zkRegistrar.stopAsync().awaitTerminated();
    model.stopAsync().awaitTerminated();
    metrics.stop();
    zooKeeperClient.close();
    try {
      stateLock.release();
    } catch (IOException e) {
      log.error("Failed to release state lock", e);
    }
    try {
      stateLockFile.close();
    } catch (IOException e) {
      log.error("Failed to close state lock file", e);
    }
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void stop() throws Exception {
    shutDown();
  }
}


/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.agent;

import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.AbstractIdleService;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.docker.client.DockerClient;
import com.spotify.helios.common.Hash;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.KafkaClientProvider;
import com.spotify.helios.servicescommon.ManagedStatsdReporter;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.ReactorFactory;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.RiemannHeartBeat;
import com.spotify.helios.servicescommon.RiemannSupport;
import com.spotify.helios.servicescommon.ServiceUtil;
import com.spotify.helios.servicescommon.ZooKeeperAclProviders;
import com.spotify.helios.servicescommon.ZooKeeperRegistrarService;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactoryImpl;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperHealthChecker;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;
import com.spotify.helios.servicescommon.coordination.ZooKeeperNodeUpdaterFactory;
import com.spotify.helios.servicescommon.statistics.Metrics;
import com.spotify.helios.servicescommon.statistics.MetricsImpl;
import com.spotify.helios.servicescommon.statistics.NoopMetrics;
import com.sun.management.OperatingSystemMXBean;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.bouncycastle.util.encoders.Base64;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.agent.Agent.EMPTY_EXECUTIONS;
import static com.spotify.helios.servicescommon.ServiceRegistrars.createServiceRegistrar;
import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * The Helios agent.
 */
public class AgentService extends AbstractIdleService implements Managed {

  private static final Logger log = LoggerFactory.getLogger(AgentService.class);

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
   * @param config The service configuration.
   * @param environment The DropWizard environment.
   * @throws ConfigurationException If an error occurs with the DropWizard configuration.
   * @throws InterruptedException If the thread is interrupted.
   */
  public AgentService(final AgentConfig config, final Environment environment)
      throws ConfigurationException, InterruptedException {
    // Create state directory, if necessary
    final Path stateDirectory = config.getStateDirectory().toAbsolutePath().normalize();
    if (!Files.exists(stateDirectory)) {
      try {
        Files.createDirectories(stateDirectory);
      } catch (IOException e) {
        log.error("Failed to create state directory: {}", stateDirectory, e);
        throw Throwables.propagate(e);
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
      throw Throwables.propagate(e);
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
      throw Throwables.propagate(e);
    }

    // Configure metrics
    final MetricRegistry metricsRegistry = new MetricRegistry();
    RiemannSupport riemannSupport = new RiemannSupport(metricsRegistry, config.getRiemannHostPort(),
                                                       config.getName(), "helios-agent");
    final RiemannFacade riemannFacade = riemannSupport.getFacade();
    if (config.isInhibitMetrics()) {
      log.info("Not starting metrics");
      metrics = new NoopMetrics();
    } else {
      log.info("Starting metrics");
      metrics = new MetricsImpl(metricsRegistry);
      environment.lifecycle().manage(new ManagedStatsdReporter(config.getStatsdHostPort(),
          "helios-agent", metricsRegistry));
      environment.lifecycle().manage(riemannSupport);
    }

    // This CountDownLatch will signal EnvironmentVariableReporter and LabelReporter when to report
    // data to ZK. They only report once and then stop, so we need to tell them when to start
    // reporting otherwise they'll race with ZooKeeperRegistrarService and might have their data
    // erased if they are too fast.
    final CountDownLatch zkRegistrationSignal = new CountDownLatch(1);

    this.zooKeeperClient = setupZookeeperClient(config, id, zkRegistrationSignal);
    final DockerHealthChecker dockerHealthChecker = new DockerHealthChecker(
        metrics.getSupervisorMetrics(), TimeUnit.SECONDS, 30, riemannFacade);
    environment.lifecycle().manage(dockerHealthChecker);
    environment.lifecycle().manage(new RiemannHeartBeat(TimeUnit.MINUTES, 2, riemannFacade));

    // Set up model
    final ZooKeeperModelReporter modelReporter =
        new ZooKeeperModelReporter(riemannFacade, metrics.getZooKeeperMetrics());
    final ZooKeeperClientProvider zkClientProvider = new ZooKeeperClientProvider(
        zooKeeperClient, modelReporter);
    final KafkaClientProvider kafkaClientProvider = new KafkaClientProvider(
        config.getKafkaBrokers());
    try {
      this.model = new ZooKeeperAgentModel(zkClientProvider, kafkaClientProvider,
        config.getName(), stateDirectory);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // Set up service registrar
    this.serviceRegistrar = createServiceRegistrar(config.getServiceRegistrarPlugin(),
                                                   config.getServiceRegistryAddress(),
                                                   config.getDomain());

    final ZooKeeperNodeUpdaterFactory nodeUpdaterFactory =
        new ZooKeeperNodeUpdaterFactory(zooKeeperClient);

    final DockerClient dockerClient;
    if (isNullOrEmpty(config.getDockerHost().dockerCertPath())) {
      dockerClient = new PollingDockerClient(config.getDockerHost().uri());
    } else {
      final Path dockerCertPath = java.nio.file.Paths.get(config.getDockerHost().dockerCertPath());
      final DockerCertificates dockerCertificates;
      try {
        dockerCertificates = new DockerCertificates(dockerCertPath);
      } catch (DockerCertificateException e) {
        throw Throwables.propagate(e);
      }

      dockerClient = new PollingDockerClient(config.getDockerHost().uri(), dockerCertificates);
    }

    final DockerClient monitoredDockerClient = MonitoredDockerClient.wrap(riemannFacade,
                                                                          dockerClient);

    this.hostInfoReporter = HostInfoReporter.newBuilder()
        .setNodeUpdaterFactory(nodeUpdaterFactory)
        .setOperatingSystemMXBean((OperatingSystemMXBean) getOperatingSystemMXBean())
        .setHost(config.getName())
        .setDockerClient(dockerClient)
        .setDockerHost(config.getDockerHost())
        .build();

    this.agentInfoReporter = AgentInfoReporter.newBuilder()
        .setNodeUpdaterFactory(nodeUpdaterFactory)
        .setRuntimeMXBean(getRuntimeMXBean())
        .setHost(config.getName())
        .build();

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

    final SupervisorFactory supervisorFactory = new SupervisorFactory(
        model, monitoredDockerClient,
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
      throw Throwables.propagate(e);
    }

    final Reaper reaper = new Reaper(dockerClient, namespace);
    this.agent = new Agent(model, supervisorFactory, reactorFactory, executions, portAllocator,
                           reaper);

    final ZooKeeperHealthChecker zkHealthChecker = new ZooKeeperHealthChecker(zooKeeperClient,
                                                                              Paths.statusHosts(),
                                                                              riemannFacade,
                                                                              TimeUnit.MINUTES, 2);
    environment.lifecycle().manage(zkHealthChecker);

    if (!config.getNoHttp()) {

      environment.healthChecks().register("docker", dockerHealthChecker);
      environment.jersey().register(new AgentModelTaskResource(model));
      environment.jersey().register(new AgentModelTaskStatusResource(model));
      environment.healthChecks().register("zookeeper", zkHealthChecker);
      environment.lifecycle().manage(this);

      this.server = ServiceUtil.createServerFactory(config.getHttpEndpoint(), config.getAdminPort(),
                                                    config.getNoHttp())
          .build(environment);
    } else {
      this.server = null;
    }
    environment.lifecycle().manage(this);
  }

  /**
   * Create a Zookeeper client and create the control and state nodes if needed.
   *
   * @param config The service configuration.
   * @return A zookeeper client.
   */
  private ZooKeeperClient setupZookeeperClient(final AgentConfig config, final String id,
                                               final CountDownLatch zkRegistrationSignal) {
    ACLProvider aclProvider = null;
    List<AuthInfo> authorization = null;

    if (config.isZooKeeperEnableAcls()) {
      final String agentPassword = config.getZooKeeperAgentPassword();
      final String masterDigest = config.getZooKeeperMasterDigest();

      if (agentPassword == null) {
        throw new HeliosRuntimeException("ZooKeeper ACLs enabled but agent password not set");
      }

      if (masterDigest == null) {
        throw new HeliosRuntimeException("ZooKeeper ACLs enabled but master digest not set");
      }

      final String agentDigest = Base64.toBase64String(Hash.sha1digest(
          String.format("%s:%s", ZooKeeperAclProviders.AGENT_USER, agentPassword).getBytes()));

      aclProvider = ZooKeeperAclProviders.defaultAclProvider(masterDigest, agentDigest);
      authorization = Lists.newArrayList(new AuthInfo(
          "digest",
          String.format("%s:%s", ZooKeeperAclProviders.AGENT_USER, agentPassword).getBytes()));
    }

    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = new CuratorClientFactoryImpl().newClient(
        config.getZooKeeperConnectionString(),
        config.getZooKeeperSessionTimeoutMillis(),
        config.getZooKeeperConnectionTimeoutMillis(),
        zooKeeperRetryPolicy,
        config.getZooKeeperNamespace(),
        aclProvider,
        authorization);

    final ZooKeeperClient client = new DefaultZooKeeperClient(curator,
                                                              config.getZooKeeperClusterId());
    client.start();

    // Register the agent
    zkRegistrar = new ZooKeeperRegistrarService(
        client,
        new AgentZooKeeperRegistrar(this, config.getName(),
                                    id, config.getZooKeeperRegistrationTtlMinutes()),
        zkRegistrationSignal);

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


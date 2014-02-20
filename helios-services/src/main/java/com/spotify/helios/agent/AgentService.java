/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.servicescommon.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.ManagedStatsdReporter;
import com.spotify.helios.servicescommon.PersistentAtomicReference;
import com.spotify.helios.servicescommon.ReactorFactory;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.RiemannSupport;
import com.spotify.helios.servicescommon.ZooKeeperNodeUpdaterFactory;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;
import com.spotify.helios.servicescommon.statistics.Metrics;
import com.spotify.helios.servicescommon.statistics.MetricsImpl;
import com.spotify.helios.servicescommon.statistics.NoopMetrics;
import com.spotify.nameless.client.Nameless;
import com.spotify.nameless.client.NamelessRegistrar;
import com.sun.management.OperatingSystemMXBean;
import com.yammer.dropwizard.config.ConfigurationException;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.config.ServerFactory;
import com.yammer.dropwizard.lifecycle.ServerLifecycleListener;
import com.yammer.metrics.core.MetricsRegistry;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static com.spotify.helios.agent.Agent.EMPTY_EXECUTIONS;
import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode.EPHEMERAL;

/**
 * The Helios agent.
 */
public class AgentService extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(AgentService.class);
  public static final byte[] EMPTY_BYTES = new byte[]{};

  private final Agent agent;

  private final Server server;
  private final ZooKeeperClient zooKeeperClient;
  private final HostInfoReporter hostInfoReporter;
  private final AgentInfoReporter agentInfoReporter;
  private final EnvironmentVariableReporter environmentVariableReporter;
  private final FileChannel stateLockFile;
  private final FileLock stateLock;
  private final ZooKeeperAgentModel model;
  private final Metrics metrics;
  private final NamelessRegistrar namelessRegistrar;
  private final MetricsRegistry metricsRegistry;
  private final Environment environment;

  private PersistentEphemeralNode upNode;
  private AgentRegistrar registrar;

  /**
   * Create a new agent instance.
   *
   * @param config The service configuration.
   */
  public AgentService(final AgentConfig config, final Environment environment)
      throws ConfigurationException {
    this.environment = environment;
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
    } catch (OverlappingFileLockException | IOException e) {
      log.error("Failed to take state lock: {}", lockPath, e);
      throw Throwables.propagate(e);
    }

    final Path idPath = config.getStateDirectory().resolve("id");
    final String id;
    try {
      if (Files.exists(idPath)) {
        id = new String(Files.readAllBytes(idPath), UTF_8);
      } else {
        final byte[] idBytes = new byte[20];
        new SecureRandom().nextBytes(idBytes);
        id = BaseEncoding.base16().encode(idBytes);
        Files.write(idPath, id.getBytes(UTF_8));
      }
    } catch (IOException e) {
      log.error("Failed to set up id file: {}", idPath, e);
      throw Throwables.propagate(e);
    }

    // Configure metrics
    metricsRegistry = new MetricsRegistry();
    RiemannSupport riemannSupport = new RiemannSupport(metricsRegistry, config.getRiemannHostPort(),
      config.getName(), "helios-agent");
    final RiemannFacade riemannFacade = riemannSupport.getFacade();
    if (config.isInhibitMetrics()) {
      log.info("Not starting metrics");
      metrics = new NoopMetrics();
    } else {
      log.info("Starting metrics");
      metrics = new MetricsImpl(metricsRegistry);
      environment.manage(new ManagedStatsdReporter(config.getStatsdHostPort(), "helios-agent",
          metricsRegistry));
      environment.manage(riemannSupport);
    }

    this.zooKeeperClient = setupZookeeperClient(config, id);
    final DockerHealthChecker healthChecker = new DockerHealthChecker(
        metrics.getSupervisorMetrics(), TimeUnit.SECONDS, 30, riemannFacade);
    environment.manage(healthChecker);

    // Set up model
    final ZooKeeperModelReporter modelReporter =
        new ZooKeeperModelReporter(riemannFacade, metrics.getZooKeeperMetrics());
    final ZooKeeperClientProvider zkClientProvider = new ZooKeeperClientProvider(zooKeeperClient,
                                                                                 modelReporter);
    try {
      this.model = new ZooKeeperAgentModel(zkClientProvider, config.getName(), stateDirectory);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    final DockerClientFactory dockerClientFactory =
        new DockerClientFactory(config.getDockerEndpoint());

    if (config.getNamelessEndpoint() != null) {
      this.namelessRegistrar = Nameless.newRegistrar(config.getNamelessEndpoint());
    } else if (config.getSite() != null) {
      this.namelessRegistrar = config.getSite().equals("localhost")
                               ? Nameless.newRegistrar("tcp://localhost:4999")
                               : Nameless.newRegistrarForDomain(config.getSite());
    } else {
      this.namelessRegistrar = null;
    }

    final ZooKeeperNodeUpdaterFactory nodeUpdaterFactory =
        new ZooKeeperNodeUpdaterFactory(zooKeeperClient);

    this.hostInfoReporter = HostInfoReporter.newBuilder()
        .setNodeUpdaterFactory(nodeUpdaterFactory)
        .setOperatingSystemMXBean((OperatingSystemMXBean) getOperatingSystemMXBean())
        .setHost(config.getName())
        .build();

    this.agentInfoReporter = AgentInfoReporter.newBuilder()
        .setNodeUpdaterFactory(nodeUpdaterFactory)
        .setRuntimeMXBean(getRuntimeMXBean())
        .setHost(config.getName())
        .build();

    this.environmentVariableReporter = new EnvironmentVariableReporter(config.getName(),
                                                                       config.getEnvVars(),
                                                                       nodeUpdaterFactory);

    final SupervisorFactory supervisorFactory = new SupervisorFactory(
        model, dockerClientFactory,
        config.getEnvVars(), namelessRegistrar,
        config.getRedirectToSyslog() != null
        ? new SyslogRedirectingCommandWrapper(config.getRedirectToSyslog())
        : new NoOpCommandWrapper(),
        config.getName(),
        metrics.getSupervisorMetrics(),
        riemannFacade);

    final ReactorFactory reactorFactory = new ReactorFactory();

    final PortAllocator portAllocator = new PortAllocator(config.getPortRangeStart(),
                                                          config.getPortRangeEnd());
    final PersistentAtomicReference<Map<JobId, Execution>> executions;
    try {
      executions = PersistentAtomicReference.create(stateDirectory.resolve("executions.json"),
                                                    new TypeReference<Map<JobId, Execution>>() {},
                                                    Suppliers.ofInstance(EMPTY_EXECUTIONS));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    this.agent = new Agent(model, supervisorFactory, reactorFactory, executions, portAllocator);

    environment.addHealthCheck(healthChecker);
    environment.addResource(new AgentModelTaskResource(model));
    environment.addResource(new AgentModelTaskStatusResource(model));
    this.server = new ServerFactory(config.getHttpConfiguration(), environment.getName())
        .buildServer(environment);
  }

  /**
   * Create a Zookeeper client and create the control and state nodes if needed.
   *
   * @param config The service configuration.
   * @param id
   * @return A zookeeper client.
   */
  private ZooKeeperClient setupZookeeperClient(final AgentConfig config, final String id) {
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = CuratorFrameworkFactory.newClient(
        config.getZooKeeperConnectionString(),
        config.getZooKeeperSessionTimeoutMillis(),
        config.getZooKeeperConnectionTimeoutMillis(),
        zooKeeperRetryPolicy);

    final ZooKeeperClient client = new DefaultZooKeeperClient(curator);

    // Register the agent
    this.registrar = new AgentRegistrar(client, config.getName(), id);
    final String upPath = Paths.statusHostUp(config.getName());

    // Create the ephemeral up node after agent registration completes
    this.registrar.getCompletionFuture().addListener(new Runnable() {
      @Override
      public void run() {
        upNode = new PersistentEphemeralNode(curator, EPHEMERAL, upPath, EMPTY_BYTES);
        upNode.start();
      }
    }, MoreExecutors.sameThreadExecutor());

    return new DefaultZooKeeperClient(curator);
  }

  @Override
  protected void startUp() throws Exception {
    zooKeeperClient.start();
    model.startAsync().awaitRunning();
    registrar.startAsync().awaitRunning();
    agent.startAsync().awaitRunning();
    hostInfoReporter.startAsync();
    agentInfoReporter.startAsync();
    environmentVariableReporter.startAsync();
    metrics.start();
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
    server.stop();
    server.join();
    hostInfoReporter.stopAsync().awaitTerminated();
    agentInfoReporter.stopAsync().awaitTerminated();
    environmentVariableReporter.stopAsync().awaitTerminated();
    agent.stopAsync().awaitTerminated();
    if (namelessRegistrar != null) {
      namelessRegistrar.shutdown().get();
    }
    registrar.stopAsync().awaitTerminated();
    model.stopAsync().awaitTerminated();
    if (upNode != null) {
      try {
        upNode.close();
      } catch (IOException e) {
        log.warn("Exception on closing up node", e.getMessage());
      }
    }
    metrics.stop();
    metricsRegistry.shutdown();
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
}


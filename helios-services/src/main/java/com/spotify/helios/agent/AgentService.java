/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;

import com.bealetech.metrics.reporting.StatsdReporter;
import com.spotify.helios.servicescommon.StatsdSupport;
import com.spotify.helios.servicescommon.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.ReactorFactory;
import com.spotify.helios.servicescommon.ZooKeeperNodeUpdaterFactory;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.statistics.Metrics;
import com.spotify.helios.servicescommon.statistics.MetricsImpl;
import com.spotify.helios.servicescommon.statistics.NoopMetrics;
import com.spotify.nameless.client.Nameless;
import com.spotify.nameless.client.NamelessRegistrar;
import com.sun.management.OperatingSystemMXBean;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
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

  private final CuratorFramework zooKeeperCurator;
  private final DefaultZooKeeperClient zooKeeperClient;
  private final HostInfoReporter hostInfoReporter;
  private final RuntimeInfoReporter runtimeInfoReporter;
  private final EnvironmentVariableReporter environmentVariableReporter;
  private final FileChannel stateLockFile;
  private final FileLock stateLock;
  private final AgentModel model;
  private final Metrics metrics;
  private final NamelessRegistrar namelessRegistrar;
  private final StatsdReporter statsdReporter;

  private PersistentEphemeralNode upNode;
  private AgentRegistrar registrar;

  /**
   * Create a new agent instance.
   *
   * @param config The service configuration.
   */
  public AgentService(final AgentConfig config) {

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
    log.info("Starting metrics");

    if (config.isInhibitMetrics()) {
      metrics = new NoopMetrics();
      statsdReporter = null;
    } else {
      metrics = new MetricsImpl(config.getMuninReporterPort());
      metrics.start();  //must be started here for statsd to be happy
      statsdReporter = StatsdSupport.getStatsdReporter(config.getStatsdHostPort(), "helios-agent");
    }

    this.zooKeeperCurator = setupZookeeperCurator(config, id);
    this.zooKeeperClient = new DefaultZooKeeperClient(zooKeeperCurator);

    this.model = setupModel(config, zooKeeperClient, stateDirectory);

    final DockerClientFactory dockerClientFactory =
        new DockerClientFactory(config.getDockerEndpoint());

    if (config.getSite() != null) {
      namelessRegistrar = config.getSite().equals("localhost")
                          ? Nameless.newRegistrar("tcp://localhost:4999")
                  : Nameless.newRegistrarForDomain(config.getSite());
    } else {
      namelessRegistrar = null;
    }

    final ZooKeeperNodeUpdaterFactory nodeUpdaterFactory =
        new ZooKeeperNodeUpdaterFactory(zooKeeperClient);

    this.hostInfoReporter = HostInfoReporter.newBuilder()
        .setNodeUpdaterFactory(nodeUpdaterFactory)
        .setOperatingSystemMXBean((OperatingSystemMXBean) getOperatingSystemMXBean())
        .setAgent(config.getName())
        .build();

    this.runtimeInfoReporter = RuntimeInfoReporter.newBuilder()
        .setNodeUpdaterFactory(nodeUpdaterFactory)
        .setRuntimeMXBean(getRuntimeMXBean())
        .setAgent(config.getName())
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
        metrics.getSupervisorMetrics());

    final ReactorFactory reactorFactory = new ReactorFactory();

    this.agent = new Agent(model, supervisorFactory, reactorFactory);
  }

  /**
   * Create a Zookeeper client and create the control and state nodes if needed.
   *
   *
   * @param config The service configuration.
   * @param id
   * @return A zookeeper client.
   */
  private CuratorFramework setupZookeeperCurator(final AgentConfig config, final String id) {
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = CuratorFrameworkFactory.newClient(
        config.getZooKeeperConnectionString(),
        config.getZooKeeperSessionTimeoutMillis(),
        config.getZooKeeperConnectionTimeoutMillis(),
        zooKeeperRetryPolicy);

//    curator.start();
    final ZooKeeperClient client = new DefaultZooKeeperClient(curator);

    // Register the agent
    this.registrar = new AgentRegistrar(client, config.getName(), id);
    final String upPath = Paths.statusAgentUp(config.getName());

    // Create the ephemeral up node after agent registration completes
    this.registrar.getCompletionFuture().addListener(new Runnable() {
      @Override
      public void run() {
        upNode = new PersistentEphemeralNode(curator, EPHEMERAL, upPath, EMPTY_BYTES);
        upNode.start();
      }
    }, MoreExecutors.sameThreadExecutor());

    return curator;
  }

  /**
   * Set up an agent state using zookeeper.
   *
   *
   * @param config          The service configuration.
   * @param zooKeeperClient The ZooKeeper client to use.
   * @param stateDirectory  The state directory to use.
   * @return An agent state.
   */
  private static AgentModel setupModel(final AgentConfig config,
                                       final DefaultZooKeeperClient zooKeeperClient,
                                       final Path stateDirectory) {
    final ZooKeeperAgentModel model;
    try {
      model = new ZooKeeperAgentModel(zooKeeperClient, config.getName(), stateDirectory);
    } catch (Exception e) {
      throw new RuntimeException("state initialization failed", e);
    }
    return model;
  }

  @Override
  protected void startUp() throws Exception {
    zooKeeperCurator.start();
    model.startAsync().awaitRunning();
    registrar.startAsync().awaitRunning();
    agent.startAsync().awaitRunning();
    hostInfoReporter.startAsync();
    runtimeInfoReporter.startAsync();
    environmentVariableReporter.startAsync();
    if (statsdReporter != null) {
      statsdReporter.start(15, TimeUnit.SECONDS);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    hostInfoReporter.stopAsync().awaitTerminated();
    runtimeInfoReporter.stopAsync().awaitTerminated();
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
    zooKeeperCurator.close();
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

    if (statsdReporter != null) {
      statsdReporter.shutdown();
    }
  }
}


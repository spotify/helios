/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.base.Throwables;

import com.spotify.helios.common.DefaultZooKeeperClient;
import com.spotify.helios.common.ReactorFactory;
import com.spotify.helios.common.coordination.RetryingZooKeeperNodeWriter;
import com.spotify.helios.common.ZooKeeperNodeUpdaterFactory;
import com.spotify.helios.common.coordination.Paths;
import com.spotify.helios.common.coordination.ZooKeeperClient;
import com.spotify.helios.common.statistics.Metrics;
import com.spotify.helios.common.statistics.MetricsImpl;
import com.spotify.helios.common.statistics.NoopMetrics;
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

import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode.EPHEMERAL;

/**
 * The Helios agent.
 */
public class AgentService {

  private static final Logger log = LoggerFactory.getLogger(AgentService.class);
  public static final byte[] EMPTY_BYTES = new byte[]{};

  private final Agent agent;

  private final CuratorFramework zooKeeperCurator;
  private final DefaultZooKeeperClient zooKeeperClient;
  private final HostInfoReporter hostInfoReporter;
  private final RuntimeInfoReporter runtimeInfoReporter;
  private final EnvironmentVariableReporter environmentVariableReporter;
  private final Path stateDirectory;
  private final FileChannel stateLockFile;
  private final FileLock stateLock;

  private PersistentEphemeralNode upNode;
  private RetryingZooKeeperNodeWriter baseNodes;

  /**
   * Create a new agent instance.
   *
   * @param config The service configuration.
   */
  public AgentService(final AgentConfig config) {

    // Create state directory, if necessary
    stateDirectory = config.getStateDirectory().toAbsolutePath().normalize();
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

    // Configure metrics
    log.info("Starting metrics");
    Metrics metrics;

    if (config.isInhibitMetrics()) {
      metrics = new NoopMetrics();
    } else {
      metrics = new MetricsImpl(config.getMuninReporterPort());
    }
    metrics.start();

    this.zooKeeperCurator = setupZookeeperCurator(config);
    this.zooKeeperClient = new DefaultZooKeeperClient(zooKeeperCurator);

    final AgentModel model = setupModel(config, zooKeeperClient, stateDirectory);

    final DockerClientFactory dockerClientFactory =
        new DockerClientFactory(config.getDockerEndpoint());

    final NamelessRegistrar registrar;
    if (config.getSite() != null) {
      registrar = config.getSite().equals("localhost")
                  ? Nameless.newRegistrar("tcp://localhost:4999")
                  : Nameless.newRegistrarForDomain(config.getSite());
    } else {
      registrar = null;
    }
    final SupervisorFactory supervisorFactory = new SupervisorFactory(model, dockerClientFactory,
        config.getEnvVars(), registrar,
        config.getRedirectToSyslog() != null
            ? new SyslogRedirectingCommandWrapper(config.getRedirectToSyslog())
            : new NoOpCommandWrapper(),
        config.getName(),
        metrics.getSupervisorMetrics());
    final ReactorFactory reactorFactory = new ReactorFactory();

    this.hostInfoReporter = HostInfoReporter.newBuilder()
        .setNodeUpdaterFactory(new ZooKeeperNodeUpdaterFactory(zooKeeperClient))
        .setOperatingSystemMXBean((OperatingSystemMXBean) getOperatingSystemMXBean())
        .setAgent(config.getName())
        .build();

    this.runtimeInfoReporter = RuntimeInfoReporter.newBuilder()
        .setNodeUpdaterFactory(new ZooKeeperNodeUpdaterFactory(zooKeeperClient))
        .setRuntimeMXBean(getRuntimeMXBean())
        .setAgent(config.getName())
        .build();

    this.environmentVariableReporter = new EnvironmentVariableReporter(config.getName(),
        config.getEnvVars(), zooKeeperClient);
    this.agent = new Agent(model, supervisorFactory, reactorFactory);
  }

  /**
   * Create a Zookeeper client and create the control and state nodes if needed.
   *
   * @param config The service configuration.
   * @return A zookeeper client.
   */
  private CuratorFramework setupZookeeperCurator(final AgentConfig config) {
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework curator = CuratorFrameworkFactory.newClient(
        config.getZooKeeperConnectionString(),
        config.getZooKeeperSessionTimeoutMillis(),
        config.getZooKeeperConnectionTimeoutMillis(),
        zooKeeperRetryPolicy);

    curator.start();
    final ZooKeeperClient client = new DefaultZooKeeperClient(curator);

    // TODO (dano): this stuff should probably live in the agent model
    final String name = config.getName();
    upNode = new PersistentEphemeralNode(curator, EPHEMERAL, Paths.statusAgentUp(name),
                                         EMPTY_BYTES);
    upNode.start();

    this.baseNodes = new RetryingZooKeeperNodeWriter(client);
    baseNodes.set(Paths.configAgentJobs(name), EMPTY_BYTES);
    baseNodes.set(Paths.configAgentPorts(name), EMPTY_BYTES);
    baseNodes.set(Paths.statusAgentJobs(name), EMPTY_BYTES);

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
      model.start();
    } catch (Exception e) {
      throw new RuntimeException("state initialization failed", e);
    }
    return model;
  }

  /**
   * Start the agent.
   */
  public void start() {
    agent.start();
    hostInfoReporter.start();
    runtimeInfoReporter.start();
    environmentVariableReporter.start();
  }

  /**
   * Stop the agent.
   */
  public void stop() throws InterruptedException {
    agent.close();
    hostInfoReporter.close();
    runtimeInfoReporter.close();
    environmentVariableReporter.close();
    baseNodes.close();
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
  }
}


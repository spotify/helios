/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.spotify.helios.common.DefaultZooKeeperClient;
import com.spotify.helios.common.ReactorFactory;
import com.spotify.helios.common.ZooKeeperNodeUpdaterFactory;
import com.spotify.helios.common.coordination.DockerClientFactory;
import com.spotify.helios.common.coordination.Paths;
import com.spotify.helios.common.coordination.ZooKeeperClient;
import com.sun.management.OperatingSystemMXBean;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.management.ManagementFactory.getOperatingSystemMXBean;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;

/**
 * The Helios agent.
 */
public class AgentService {

  private static final Logger log = LoggerFactory.getLogger(AgentService.class);

  private final Agent agent;

  private final CuratorFramework zooKeeperCurator;
  private final DefaultZooKeeperClient zooKeeperClient;
  private final HostInfoReporter hostInfoReporter;
  private final RuntimeInfoReporter runtimeInfoReporter;

  /**
   * Create a new agent instance.
   *
   * @param config The service configuration.
   */
  public AgentService(final AgentConfig config) {

    final MetricsRegistry metricsRegistry = Metrics.defaultRegistry();

    this.zooKeeperCurator = setupZookeeperCurator(config);
    this.zooKeeperClient = new DefaultZooKeeperClient(zooKeeperCurator);

    final AgentModel model = setupState(config, zooKeeperClient);

    final DockerClientFactory dockerClientFactory =
        new DockerClientFactory(config.getDockerEndpoint());
    final SupervisorFactory supervisorFactory = new SupervisorFactory(model, dockerClientFactory,
                                                                      config);
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
    final CuratorFramework client = CuratorFrameworkFactory.newClient(
        config.getZooKeeperConnectionString(),
        config.getZooKeeperSessionTimeoutMillis(),
        config.getZooKeeperConnectionTimeoutMillis(),
        zooKeeperRetryPolicy);

    client.start();
    final ZooKeeperClient curator = new DefaultZooKeeperClient(client);

    try {
      // TODO: this logic should probably live in the agent

      final String name = config.getName();
      curator.ensurePath(Paths.configAgentJobs(name));
      curator.ensurePath(Paths.statusAgentJobs(name));

      final String upNode = Paths.statusAgentUp(name);
      if (curator.stat(upNode) != null) {
        curator.delete(upNode);
      }
      curator.createWithMode(upNode, EPHEMERAL);
    } catch (KeeperException e) {
      throw new RuntimeException("zookeeper initialization failed", e);
    }

    return client;
  }

  /**
   * Set up an agent state using zookeeper.
   *
   * @param config          The service configuration.
   * @param zooKeeperClient The ZooKeeper client to use.
   * @return An agent state.
   */
  private static AgentModel setupState(final AgentConfig config,
                                       final DefaultZooKeeperClient zooKeeperClient) {
    final ZooKeeperAgentModel state = new ZooKeeperAgentModel(zooKeeperClient, config.getName());
    try {
      state.start();
    } catch (Exception e) {
      throw new RuntimeException("state initialization failed", e);
    }
    return state;
  }

  /**
   * Start the agent.
   */
  public void start() {
    agent.start();
    hostInfoReporter.start();
    runtimeInfoReporter.start();
  }

  /**
   * Stop the agent.
   */
  public void stop() {
    agent.close();
    hostInfoReporter.close();
    runtimeInfoReporter.close();
    zooKeeperCurator.close();
  }
}


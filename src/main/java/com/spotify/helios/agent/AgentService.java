/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.spotify.helios.common.ZooKeeperCurator;
import com.spotify.helios.common.coordination.*;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;

/**
 * The Helios agent.
 */
public class AgentService {

  private static final Logger log = LoggerFactory.getLogger(AgentService.class);

  private final Worker worker;

  private final CuratorFramework zooKeeperClient;

  /**
   * Create a new agent instance.
   *
   * @param config The service configuration.
   */
  public AgentService(final AgentConfig config) {

    final MetricsRegistry metricsRegistry = Metrics.defaultRegistry();

    zooKeeperClient = setupZookeeperClient(config);

    final State state = setupState(config);

    final DockerClientFactory dockerClientFactory =
        new DockerClientFactory(config.getDockerEndpoint());
    final JobRunnerFactory jobRunnerFactory = new JobRunnerFactory(state, dockerClientFactory);

    worker = new Worker(state, jobRunnerFactory);
  }

  /**
   * Create a Zookeeper client and create the control and state nodes if needed.
   *
   * @param config The service configuration.
   * @return A zookeeper client.
   */
  private CuratorFramework setupZookeeperClient(final AgentConfig config) {
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework client = CuratorFrameworkFactory.newClient(
        config.getZooKeeperConnectionString(), zooKeeperRetryPolicy);

    client.start();
    final CuratorInterface curator = new ZooKeeperCurator(client);

    try {
      // TODO: this logic should probably live in the worker
      curator.ensurePath(format("/config/agents/%s/jobs", config.getName()));
      curator.ensurePath(format("/status/agents/%s/jobs", config.getName()));
      final String upNode = format("/status/agents/%s/up", config.getName());
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
   * Set up a worker state using zookeeper.
   *
   * @param config The service configuration.
   * @return A worker state.
   */
  private State setupState(final AgentConfig config) {
    final CuratorInterface curator = new ZooKeeperCurator(zooKeeperClient);
    final ZooKeeperState state = new ZooKeeperState(curator, config.getName());
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
  }

  /**
   * Stop the agent.
   */
  public void stop() {
    worker.close();

    if (zooKeeperClient != null) {
      zooKeeperClient.close();
    }

    worker.close();
  }
}


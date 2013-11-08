/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.spotify.helios.common.AbstractClient;
import com.spotify.helios.common.coordination.Coordinator;
import com.spotify.helios.common.coordination.CuratorInterface;
import com.spotify.helios.common.ZooKeeperCurator;
import com.spotify.helios.master.http.HttpServiceRequest;
import com.spotify.hermes.Hermes;
import com.spotify.hermes.http.HermesHttpRequestDispatcher;
import com.spotify.hermes.http.HttpServer;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.service.ReplyHandler;
import com.spotify.hermes.service.RequestHandler;
import com.spotify.hermes.service.Server;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import static com.spotify.hermes.message.ProtocolVersion.V2;

/**
 * The Helios master service.
 */
public class MasterService {

  private static final Logger log = LoggerFactory.getLogger(MasterService.class);

  private final CuratorFramework zooKeeperClient;

  private final Server hermesServer;
  private final String hermesEndpoint;
  private final HttpServer httpServer;
  private final InetSocketAddress httpEndpoint;

  /**
   * Create a new service instance. Initializes the control interface and the worker.
   *
   * @param config The service configuration.
   */
  public MasterService(final MasterConfig config) {

    hermesEndpoint = config.getHermesEndpoint();

    // Set up statistics
    final MetricsRegistry metricsRegistry = Metrics.defaultRegistry();

    // Set up clients
    zooKeeperClient = setupZookeeperClient(config);

    // Set up the master interface
    final ZooKeeperCurator curator = new ZooKeeperCurator(zooKeeperClient);
    final Coordinator coordinator = new ZooKeeperCoordinator(curator);
    final MasterHandler masterHandler = new MasterHandler(coordinator);

    // master server
    hermesServer = Hermes.newServer(masterHandler);
    httpEndpoint = config.getHttpEndpoint();
    final com.spotify.hermes.http.Statistics statistics = new com.spotify.hermes.http.Statistics();
    // TODO: this is a bit messy
    final HermesHttpRequestDispatcher requestDispatcher =
        new HermesHttpRequestDispatcher(new RequestHandlerClient(masterHandler), statistics, V2, 30000,
                                        "helios");
    httpServer = new HttpServer(requestDispatcher, new HttpServer.Config(), statistics);
  }

  /**
   * Create a Zookeeper client and create the control and state nodes if needed.
   *
   * @param config The service configuration.
   * @return A zookeeper client.
   */
  private CuratorFramework setupZookeeperClient(final MasterConfig config) {
    final RetryPolicy zooKeeperRetryPolicy = new ExponentialBackoffRetry(1000, 3);
    final CuratorFramework client = CuratorFrameworkFactory.newClient(
        config.getZooKeeperConnectString(), zooKeeperRetryPolicy);
    client.start();

    final CuratorInterface curator = new ZooKeeperCurator(client);

    try {
      curator.ensurePath("/config/agents");
      curator.ensurePath("/config/jobs");
      curator.ensurePath("/status/agents");
    } catch (KeeperException e) {
      throw new RuntimeException("zookeeper initialization failed", e);
    }

    return client;
  }

  /**
   * Start the service. Binds the control interfaces.
   */
  public void start() {
    log.info("hermes: " + hermesEndpoint);
    log.info("http: {}:{}", httpEndpoint.getHostString(), httpEndpoint.getPort());
    hermesServer.bind(hermesEndpoint);
    httpServer.bind(httpEndpoint);
  }

  /**
   * Stop the service. Tears down the control interfaces.
   */
  public void stop() {
    if (hermesServer != null) {
      hermesServer.stop();
    }

    if (httpServer != null) {
      httpServer.stop();
    }

    if (zooKeeperClient != null) {
      zooKeeperClient.close();
    }
  }

  // TODO: move
  private class RequestHandlerClient extends AbstractClient {

    private final RequestHandler requestHandler;

    private RequestHandlerClient(final RequestHandler requestHandler) {
      this.requestHandler = requestHandler;
    }

    @Override
    public void send(final Message request, final ReplyHandler replyHandler) {
      requestHandler.handleRequest(new HttpServiceRequest(request, replyHandler));
    }
  }
}

/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import com.bealetech.metrics.reporting.StatsdReporter;
import com.spotify.helios.common.AbstractClient;
import com.spotify.helios.master.http.HttpServiceRequest;
import com.spotify.helios.servicescommon.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.RiemannSupport;
import com.spotify.helios.servicescommon.StatsdSupport;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.statistics.Metrics;
import com.spotify.helios.servicescommon.statistics.MetricsImpl;
import com.spotify.helios.servicescommon.statistics.NoopMetrics;
import com.spotify.hermes.Hermes;
import com.spotify.hermes.http.HermesHttpRequestDispatcher;
import com.spotify.hermes.http.HttpServer;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.service.ReplyHandler;
import com.spotify.hermes.service.RequestHandler;
import com.spotify.hermes.service.Server;
import com.spotify.nameless.client.Nameless;
import com.spotify.nameless.client.NamelessRegistrar;
import com.spotify.nameless.client.RegistrationHandle;
import com.yammer.metrics.reporting.RiemannReporter;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Throwables.propagate;
import static com.spotify.hermes.message.ProtocolVersion.V2;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;

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
  private final NamelessRegistrar registrar;
  private final Metrics metrics;
  private final StatsdReporter statsdReporter;
  private final RiemannFacade riemannFacade;
  private final RiemannReporter riemannReporter;

  private RegistrationHandle namelessHermesHandle;
  private RegistrationHandle namelessHttpHandle;


  /**
   * Create a new service instance. Initializes the control interface and the worker.
   *
   * @param config The service configuration.
   * @throws IOException
   */
  public MasterService(final MasterConfig config) throws IOException {
    this.hermesEndpoint = config.getHermesEndpoint();

    RiemannSupport riemannSupport = new RiemannSupport(config.getRiemannHostPort(), "helios-master");
    riemannFacade = riemannSupport.getFacade();

    // Configure metrics
    log.info("Starting metrics");
    if (config.isInhibitMetrics()) {
      metrics = new NoopMetrics();
      statsdReporter = null;
      riemannReporter = null;
    } else {
      metrics = new MetricsImpl();
      metrics.start(); //must be started here for statsd to be happy
      statsdReporter = StatsdSupport.getStatsdReporter(config.getStatsdHostPort(), "helios-master");
      riemannReporter = riemannSupport.getReporter();
    }

    // Set up clients
    this.zooKeeperClient = setupZookeeperClient(config);

    // Set up the master interface
    final DefaultZooKeeperClient curator = new DefaultZooKeeperClient(zooKeeperClient);
    final MasterModel model = new ZooKeeperMasterModel(curator);
    final MasterHandler handler = new MasterHandler(model, metrics.getMasterMetrics(),
        riemannFacade);

    // master server
    this.hermesServer = Hermes.newServer(handler);
    this.httpEndpoint = config.getHttpEndpoint();
    final com.spotify.hermes.http.Statistics statistics = new com.spotify.hermes.http.Statistics();
    // TODO: this is a bit messy
    final HermesHttpRequestDispatcher requestDispatcher =
        new HermesHttpRequestDispatcher(new RequestHandlerClient(handler), statistics, V2,
                                        30000,
                                        "helios");
    this.httpServer = new HttpServer(requestDispatcher, new HttpServer.Config(), statistics);

    if (config.getSite() != null)  {
      this.registrar =
          config.getSite().equals("localhost") ?
          Nameless.newRegistrar("tcp://localhost:4999") :
          Nameless.newRegistrarForDomain(config.getSite());
    } else {
      this.registrar = null;
    }
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

    final ZooKeeperClient curator = new DefaultZooKeeperClient(client);

    // TODO (dano): move directory initialization elsewhere
    try {
      curator.ensurePath(Paths.configAgents());
      curator.ensurePath(Paths.configJobs());
      curator.ensurePath(Paths.configJobRefs());
      curator.ensurePath(Paths.statusAgents());
      curator.ensurePath(Paths.statusMasters());
      curator.ensurePath(Paths.historyJobs());

      final String upNode = Paths.statusMasterUp(config.getName());
      curator.ensurePath(upNode, true);
      if (curator.stat(upNode) != null) {
        curator.delete(upNode);
      }
      curator.createWithMode(upNode, EPHEMERAL);
    } catch (KeeperException e) {
      throw new RuntimeException("zookeeper initialization failed", e);
    }

    return client;
  }

  int getHermesPort(String s) {
    return Integer.valueOf(Iterables.getLast(Splitter.on(":").split(s)));
  }

  /**
   * Start the service. Binds the control interfaces.
   */
  public void start() {
    log.info("hermes: " + hermesEndpoint);
    log.info("http: {}:{}", httpEndpoint.getHostString(), httpEndpoint.getPort());
    hermesServer.bind(hermesEndpoint);
    httpServer.bind(httpEndpoint);

    final int hermesPort = getHermesPort(hermesEndpoint);
    log.info("hermes port: {}", hermesPort);
    if (this.registrar != null) {
      try {
        log.info("registering with nameless");
        namelessHermesHandle = registrar.register("helios", "hm", hermesPort).get();
        namelessHttpHandle = registrar.register("helios", "http", httpEndpoint.getPort()).get();
      } catch (InterruptedException | ExecutionException e) {
        throw propagate(e);
      }
    }
    if (statsdReporter != null) {
      statsdReporter.start(15, TimeUnit.SECONDS);
    }
    if (riemannReporter != null) {
      riemannReporter.start(15, TimeUnit.SECONDS);
    }
  }

  /**
   * Stop the service. Tears down the control interfaces.
   */
  public void stop() {
    if (registrar != null) {
      if (namelessHttpHandle != null) {
        registrar.unregister(namelessHttpHandle);
      }

      if (namelessHermesHandle != null) {
        registrar.unregister(namelessHermesHandle);
      }

      registrar.shutdown();
    }

    hermesServer.stop();
    httpServer.stop();
    zooKeeperClient.close();

    if (statsdReporter != null) {
      statsdReporter.shutdown();
    }
    if (riemannReporter != null) {
      riemannReporter.shutdown();
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

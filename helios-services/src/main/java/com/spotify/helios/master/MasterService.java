/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package com.spotify.helios.master;

import ch.qos.logback.access.jetty.RequestLogImpl;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.AbstractIdleService;

import com.spotify.helios.master.http.VersionResponseFilter;
import com.spotify.helios.master.metrics.ReportingResourceMethodDispatchAdapter;
import com.spotify.helios.master.resources.HistoryResource;
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
import com.spotify.helios.servicescommon.ServiceUtil;
import com.spotify.helios.servicescommon.ZooKeeperRegistrar;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactory;
import com.spotify.helios.servicescommon.coordination.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClientProvider;
import com.spotify.helios.servicescommon.coordination.ZooKeeperHealthChecker;
import com.spotify.helios.servicescommon.coordination.ZooKeeperModelReporter;
import com.spotify.helios.servicescommon.statistics.Metrics;
import com.spotify.helios.servicescommon.statistics.MetricsImpl;
import com.spotify.helios.servicescommon.statistics.NoopMetrics;
import com.yammer.metrics.core.MetricsRegistry;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.jetty.RequestLogFactory;
import io.dropwizard.logging.AppenderFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.setup.Environment;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import javax.servlet.DispatcherType;

import static com.google.common.base.Charsets.UTF_8;
import static com.spotify.helios.servicescommon.ServiceRegistrars.createServiceRegistrar;

/**
 * The Helios master service.
 */
public class MasterService extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(MasterService.class);

  private final Server server;
  private final MasterConfig config;
  private final ServiceRegistrar registrar;
  private final RiemannFacade riemannFacade;
  private final ZooKeeperClient zooKeeperClient;
  private final ExpiredJobReaper expiredJobReaper;
  private final CuratorClientFactory curatorClientFactory;

  private ZooKeeperRegistrar zkRegistrar;

  /**
   * Create a new service instance. Initializes the control interface and the worker.
   *
   * @param config The service configuration.
   */
  public MasterService(final MasterConfig config, final Environment environment,
                       final CuratorClientFactory curatorClientFactory)
      throws ConfigurationException {
    this.config = config;
    this.curatorClientFactory = curatorClientFactory;

    // Configure metrics
    // TODO (dano): do something with the riemann facade
    final MetricsRegistry metricsRegistry = com.yammer.metrics.Metrics.defaultRegistry();
    final RiemannSupport riemannSupport = new RiemannSupport(metricsRegistry,
        config.getRiemannHostPort(), config.getName(), "helios-master");
    riemannFacade = riemannSupport.getFacade();
    log.info("Starting metrics");
    final Metrics metrics;
    if (config.isInhibitMetrics()) {
      metrics = new NoopMetrics();
    } else {
      metrics = new MetricsImpl(metricsRegistry);
      metrics.start();
      environment.lifecycle().manage(riemannSupport);
      environment.lifecycle().manage(new ManagedStatsdReporter(config.getStatsdHostPort(),
          "helios-master", metricsRegistry));
    }

    // Set up the master model
    this.zooKeeperClient = setupZookeeperClient(config);
    final ZooKeeperModelReporter modelReporter = new ZooKeeperModelReporter(
        riemannFacade, metrics.getZooKeeperMetrics());
    final ZooKeeperClientProvider zkClientProvider = new ZooKeeperClientProvider(
        zooKeeperClient, modelReporter);
    final MasterModel model = new ZooKeeperMasterModel(zkClientProvider);

    final ZooKeeperHealthChecker zooKeeperHealthChecker = new ZooKeeperHealthChecker(
        zooKeeperClient, Paths.statusMasters(), riemannFacade, TimeUnit.MINUTES, 2);

    environment.lifecycle().manage(zooKeeperHealthChecker);
    environment.healthChecks().register("zookeeper", zooKeeperHealthChecker);
    environment.lifecycle().manage(new RiemannHeartBeat(TimeUnit.MINUTES, 2, riemannFacade));

    // Set up service registrar
    this.registrar = createServiceRegistrar(config.getServiceRegistrarPlugin(),
                                            config.getServiceRegistryAddress(),
                                            config.getDomain());

    // Set up reaping of expired jobs
    this.expiredJobReaper = ExpiredJobReaper.newBuilder()
        .setMasterModel(model)
        .build();

    // Set up http server
    environment.servlets()
        .addFilter("VersionResponseFilter", VersionResponseFilter.class)
        .addMappingForUrlPatterns(EnumSet.of(DispatcherType.REQUEST), true, "/*");
    environment.jersey().register(
        new ReportingResourceMethodDispatchAdapter(metrics.getMasterMetrics()));
    environment.jersey().register(new JobsResource(model, metrics.getMasterMetrics()));
    environment.jersey().register(new HistoryResource(model, metrics.getMasterMetrics()));
    environment.jersey().register(new HostsResource(model));
    environment.jersey().register(new MastersResource(model));
    environment.jersey().register(new VersionResource());

    final DefaultServerFactory serverFactory = ServiceUtil.createServerFactory(
        config.getHttpEndpoint(), config.getAdminPort(), false);

    final RequestLogFactory requestLog = new RequestLogFactory();
    requestLog.setAppenders(ImmutableList.<AppenderFactory>of());
    serverFactory.setRequestLogFactory(requestLog);

    this.server = serverFactory.build(environment);

    setUpRequestLogging();
  }

  private final void setUpRequestLogging() {
    // Set up request logging
    final Handler originalHandler = server.getHandler();
    final HandlerCollection handlerCollection;
    if (originalHandler instanceof HandlerCollection) {
      handlerCollection = (HandlerCollection) originalHandler;
    } else {
      handlerCollection = new HandlerCollection();
      handlerCollection.addHandler(originalHandler);
    }

    final RequestLogHandler requestLogHandler = new RequestLogHandler();
    final RequestLogImpl requestLog = new RequestLogImpl();
    requestLog.setQuiet(true);
    requestLog.setResource("/logback-access.xml");
    requestLogHandler.setRequestLog(requestLog);
    handlerCollection.addHandler(requestLogHandler);
    server.setHandler(handlerCollection);
  }

  @Override
  protected void startUp() throws Exception {
    logBanner();
    zkRegistrar.startAsync().awaitRunning();
    expiredJobReaper.startAsync().awaitRunning();
    try {
      server.start();
    } catch (Exception e) {
      log.error("Unable to start server, shutting down", e);
      server.stop();
    }

    final ServiceRegistration serviceRegistration = ServiceRegistration.newBuilder()
        .endpoint("helios", "http", config.getHttpEndpoint().getPort(),
            config.getDomain(), config.getName())
        .build();
    registrar.register(serviceRegistration);
  }

  @Override
  protected void shutDown() throws Exception {
    server.stop();
    server.join();
    registrar.close();
    expiredJobReaper.stopAsync().awaitTerminated();
    zkRegistrar.stopAsync().awaitTerminated();
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
    final CuratorFramework curator = curatorClientFactory.newClient(
        config.getZooKeeperConnectionString(),
        config.getZooKeeperSessionTimeoutMillis(),
        config.getZooKeeperConnectionTimeoutMillis(),
        zooKeeperRetryPolicy,
        config.getZooKeeperNamespace());
    final ZooKeeperClient client = new DefaultZooKeeperClient(curator,
                                                              config.getZooKeeperClusterId());
    client.start();
    zkRegistrar = new ZooKeeperRegistrar(client, new MasterZooKeeperRegistrar(config.getName()));

    return client;
  }
}

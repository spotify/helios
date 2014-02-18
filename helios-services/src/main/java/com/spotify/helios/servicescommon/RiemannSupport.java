package com.spotify.helios.servicescommon;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import com.aphyr.riemann.client.RiemannClient;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.RiemannReporter;
import com.yammer.metrics.reporting.RiemannReporter.Config;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class RiemannSupport implements Managed {
  private final String host;
  private final int port;
  private final MetricsRegistry metricsRegistry;
  private final String serviceName;
  private final String hostName;

  private RiemannClient client = null;
  private RiemannReporter riemannReporter;

  public RiemannSupport(final MetricsRegistry metricsRegistry, final String hostPort,
                        final String hostName, final String serviceName) {
    this.metricsRegistry = metricsRegistry;
    this.serviceName = serviceName;
    this.hostName = hostName;
    if (Strings.isNullOrEmpty(hostPort)) {
      host = null;
      port = 0;
      return;
    }
    Iterable<String> parts = Splitter.on(":").split(hostPort);
    if (Iterables.size(parts) != 2) {
      throw new RuntimeException(
          "specification of riemann host port has wrong number of parts.  Should be host:port");
    }
    host = Iterables.get(parts, 0);
    port = Integer.valueOf(Iterables.get(parts, 1));
  }

  public RiemannFacade getFacade() {
    RiemannClient cli = getClient();
    if (cli == null) {
      return NoOpRiemannClient.facade();
    }
    return new RiemannFacade(cli, hostName, serviceName);
  }

  private RiemannClient getClient() {
    if (host == null) {
      return null;
    }

    if (client == null) {
      try {
        client = RiemannClient.udp(host, port);
        client.connect();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    return client;
  }

  public RiemannReporter getReporter() {
    if (host == null) {
      return null;
    }
    Config c = Config.newBuilder()
        .metricsRegistry(metricsRegistry)
        .name(serviceName)
        .localHost(hostName)
        .host(host)
        .port(port)
        .build();
    return new RiemannReporter(c, getClient());
  }

  @Override
  public void start() throws Exception {
    riemannReporter = getReporter();
    if (riemannReporter != null) {
      riemannReporter.start(15, SECONDS);
    }
  }

  @Override
  public void stop() throws Exception {
    if (riemannReporter != null) {
      riemannReporter.shutdown();
    }
  }
}

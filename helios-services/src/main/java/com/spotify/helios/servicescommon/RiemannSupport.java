package com.spotify.helios.servicescommon;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import com.aphyr.riemann.client.RiemannClient;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.RiemannReporter;
import com.yammer.metrics.reporting.RiemannReporter.Config;

import java.io.IOException;

public class RiemannSupport {
  private final String host;
  private final int port;
  private final MetricsRegistry metricsRegistry;
  private final String name;
  private RiemannClient client = null;

  public RiemannSupport(final MetricsRegistry metricsRegistry, final String hostPort,
                        final String name) {
    this.metricsRegistry = metricsRegistry;
    this.name = name;
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
    return new RiemannFacade(cli);
  }

  private RiemannClient getClient() {
    if (host == null) {
      return null;
    }

    if (client == null) {
      try {
        client = RiemannClient.udp(host, port);
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
        .name(name)
        .host(host)
        .port(port)
        .build();
    return new RiemannReporter(c, getClient());
  }
}

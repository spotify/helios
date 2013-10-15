/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.common;

import com.spotify.hermes.Hermes;
import com.spotify.statistics.JvmMetrics;
import com.spotify.statistics.MuninReporter;
import com.spotify.statistics.MuninReporterConfig;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;

import java.net.InetAddress;

public class MuninReporterFactory {

  /**
   * Create and conditionally start munin reporter.
   *
   * @param port The port to use. If zero, the munin reporter won't be started.
   * @return A running {@link com.spotify.statistics.MuninReporter} if port is not zero, null
   *         otherwise.
   */
  public static MuninReporter setupMuninReporter(final int port) {
    final InetAddress muninAddress = InetAddress.getLoopbackAddress();
    final MetricsRegistry metricsRegistry = Metrics.defaultRegistry();
    final MuninReporterConfig muninReporterConfig = new MuninReporterConfig(metricsRegistry);
    JvmMetrics.register(metricsRegistry, muninReporterConfig.category("Helios - JVM"));
    Hermes.configureServerGraphs(muninReporterConfig.category("Helios - Hermes Server"));
    Hermes.configureClientGraphs(muninReporterConfig.category("Helios - Hermes Client"));
    final MuninReporter muninReporter = new MuninReporter(metricsRegistry, port, muninAddress,
                                                          muninReporterConfig.build());

    if (port != 0) {
      muninReporter.start();
    }

    return muninReporter;
  }
}

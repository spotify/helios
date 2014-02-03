package com.spotify.helios.servicescommon;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import com.bealetech.metrics.reporting.StatsdReporter;
import com.yammer.dropwizard.lifecycle.Managed;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ManagedStatsdReporter implements Managed {

  private static final int POLL_INTERVAL_SECONDS = 15;
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

  private final StatsdReporter statsdReporter;

  public ManagedStatsdReporter(final String endpoint, final String name) {
    if (Strings.isNullOrEmpty(endpoint)) {
      statsdReporter = null;
      return;
    }
    final List<String> parts = Splitter.on(":").splitToList(endpoint);
    checkArgument(parts.size() == 2, "Specification of statsd host port has wrong number of " +
                                     "parts. Should be host:port");
    final String host = parts.get(0);
    final int port = Integer.valueOf(parts.get(1));
    try {
      statsdReporter = new StatsdReporter(host, port, name);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void start() throws Exception {
    if (statsdReporter != null) {
      statsdReporter.start(POLL_INTERVAL_SECONDS, SECONDS);
    }
  }

  @Override
  public void stop() throws Exception {
    if (statsdReporter != null) {
      statsdReporter.shutdown(SHUTDOWN_TIMEOUT_SECONDS, SECONDS);
    }
  }
}

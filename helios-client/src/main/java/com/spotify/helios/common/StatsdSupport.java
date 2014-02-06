package com.spotify.helios.common;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import com.bealetech.metrics.reporting.StatsdReporter;

import java.io.IOException;

public class StatsdSupport {
  public static StatsdReporter getStatsdReporter(String hostPort, String name) {
    if (Strings.isNullOrEmpty(hostPort)) {
      return null;
    }
    Iterable<String> parts = Splitter.on(":").split(hostPort);
    if (Iterables.size(parts) != 2) {
      throw new RuntimeException(
        "specification of statsd host port has wrong number of parts.  Should be host:port");
    }
    String host = Iterables.get(parts, 0);
    int port = Integer.valueOf(Iterables.get(parts, 1));
    StatsdReporter statsdReporter;
    try {
      statsdReporter = new StatsdReporter(host, port, name);
    } catch (IOException e) {
      // Looking at the source for StatsdReporter, this shouldn't actually happen....?
      throw new RuntimeException(e);
    }
    return statsdReporter;
  }
}

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

package com.spotify.helios.servicescommon;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import com.aphyr.riemann.client.RiemannClient;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.riemann.Riemann;
import com.codahale.metrics.riemann.RiemannReporter;

import io.dropwizard.lifecycle.Managed;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Ties together construction of the Riemann client, metrics reporting via Riemann and exposing
 * the facade.
 */
public class RiemannSupport implements Managed {
  private final String host;
  private final int port;
  private final MetricRegistry metricsRegistry;
  private final String serviceName;
  private final String hostName;

  private RiemannClient client = null;
  private RiemannReporter riemannReporter;
  private final String proto;

  public RiemannSupport(final MetricRegistry metricsRegistry, final String hostPort,
                        final String hostName, final String serviceName) {
    this.metricsRegistry = metricsRegistry;
    this.serviceName = serviceName;
    this.hostName = hostName;
    if (Strings.isNullOrEmpty(hostPort)) {
      host = null;
      port = 0;
      proto = null;
      return;
    }
    final Iterable<String> parts = Splitter.on(":").split(hostPort);
    final int size = Iterables.size(parts);
    if (size > 3 || size < 2) {
      throw new RuntimeException(
          "specification of riemann host port has wrong number of parts.  Should be"
          + " [proto:]host:port, where proto is udp or tcp");
    }
    if (size == 3) {
      this.proto = Iterables.get(parts, 0);
    } else {
      this.proto = "udp";
    }
    checkState("udp".equals(this.proto) || "tcp".equals(this.proto));
    host = Iterables.get(parts, size - 2);
    port = Integer.valueOf(Iterables.get(parts, size - 1));
  }

  public RiemannFacade getFacade() {
    final RiemannClient cli = getClient();
    if (cli == null) {
      return new NoOpRiemannClient().facade();
    }
    return new RiemannFacade(cli, hostName, serviceName);
  }

  private RiemannClient getClient() {
    if (host == null) {
      return null;
    }

    if (client == null) {
      try {
        if ("udp".equals(proto)) {
          client = RiemannClient.udp(host, port);
        } else if ("tcp".equals(proto)) {
          client = RiemannClient.tcp(host, port);
        } else {
          throw new IllegalArgumentException();
        }
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
    return RiemannReporter.forRegistry(metricsRegistry)
        .localHost(hostName)
        .build(new Riemann(getClient()));
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
      riemannReporter.close();
    }
  }
}

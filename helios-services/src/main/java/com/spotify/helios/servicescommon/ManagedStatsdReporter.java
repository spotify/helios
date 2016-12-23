/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.servicescommon;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.readytalk.metrics.StatsDReporter;
import io.dropwizard.lifecycle.Managed;
import java.util.List;

public class ManagedStatsdReporter implements Managed {

  private static final int POLL_INTERVAL_SECONDS = 15;

  private final StatsDReporter statsdReporter;

  public ManagedStatsdReporter(final String endpoint, final MetricRegistry registry) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(endpoint));

    final List<String> parts = Splitter.on(":").splitToList(endpoint);
    checkArgument(parts.size() == 2, "Specification of statsd host port has wrong number of "
                                     + "parts. Should be host:port");
    final String host = parts.get(0);
    final int port = Integer.valueOf(parts.get(1));
    statsdReporter = StatsDReporter.forRegistry(registry).build(host, port);
  }

  @Override
  public void start() throws Exception {
    statsdReporter.start(POLL_INTERVAL_SECONDS, SECONDS);
  }

  @Override
  public void stop() throws Exception {
    statsdReporter.close();
  }
}

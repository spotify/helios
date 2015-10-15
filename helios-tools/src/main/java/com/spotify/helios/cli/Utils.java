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

package com.spotify.helios.cli;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostSelector;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;

public class Utils {

  public static <K, V> Map<K, V> allAsMap(final Map<K, ListenableFuture<V>> map)
      throws ExecutionException, InterruptedException {
    final Map<K, V> result = Maps.newHashMap();
    for (Map.Entry<K, ListenableFuture<V>> e : map.entrySet()) {
      result.put(e.getKey(), e.getValue().get());
    }
    return result;
  }

  public static HeliosClient getClient(final Target target, final PrintStream err,
                                       final String username, final Path authPlugin,
                                       final Path privateKeyPath) {

    List<URI> endpoints = Collections.emptyList();
    try {
      endpoints = target.getEndpointSupplier().get();
    } catch (Exception ignore) {
      // TODO (dano): Nasty. Refactor target to propagate resolution failure in a checked manner.
    }
    if (endpoints.size() == 0) {
      err.println("Failed to resolve helios master in " + target);
      return null;
    }

    return HeliosClient.newBuilder()
        .setEndpointSupplier(target.getEndpointSupplier())
        .setUser(username)
        .setAuthPlugin(authPlugin)
        .setPrivateKeyPath(privateKeyPath)
        .build();
  }

  public static boolean userConfirmed(final PrintStream out, final BufferedReader stdin)
      throws IOException {
    out.printf("Do you want to continue? [y/N]%n");

    final String line = stdin.readLine().trim();

    if (line.length() < 1) {
      return false;
    }
    final char c = line.charAt(0);

    return !(c != 'Y' && c != 'y');

  }

  public static Map<String, String> argToStringMap(final Namespace namespace, final Argument arg) {
    final List<List<String>> args = namespace.getList(arg.getDest());
    final Map<String, String> map = Maps.newHashMap();
    if (args != null) {
      for (final List<String> group : args) {
        for (final String s : group) {
          final String[] parts = s.split("=", 2);
          if (parts.length != 2) {
            throw new IllegalArgumentException("Bad " + arg.textualName() + " value: " + s);
          }
          map.put(parts[0], parts[1]);
        }
      }
    }
    return map;
  }

  public static List<HostSelector> parseHostSelectors(final Namespace namespace,
                                                      final Argument arg) {
    final List<List<String>> args = namespace.getList(arg.getDest());
    final List<HostSelector> ret = Lists.newArrayList();
    if (args != null) {
      for (final List<String> group : args) {
        for (final String s : group) {
          final HostSelector hostSelector = HostSelector.parse(s);
          if (hostSelector == null) {
            throw new IllegalArgumentException(format("Bad host selector expression: '%s'", s));
          }
          ret.add(hostSelector);
        }
      }
    }
    return ret;
  }
}

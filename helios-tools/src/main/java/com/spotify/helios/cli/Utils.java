/*-
 * -\-\-
 * Helios Tools
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

package com.spotify.helios.cli;

import static java.lang.String.format;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.spotify.helios.client.Endpoints;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.protocol.SetGoalResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Utils {

  private static final Logger log = LoggerFactory.getLogger(Utils.class);

  static final String HTTP_TIMEOUT_ENV_VAR = "HELIOS_CLI_HTTP_TIMEOUT";
  static final int DEFAULT_HTTP_TIMEOUT_SECS = 30;

  static final String TOTAL_TIMEOUT_ENV_VAR = "HELIOS_CLI_TOTAL_TIMEOUT";
  static final int DEFAULT_TOTAL_TIMEOUT_SECS = DEFAULT_HTTP_TIMEOUT_SECS * 4;

  private Utils() {
  }

  public static HeliosClient getClient(final Target target, final PrintStream err,
                                       final String username, final Namespace options) {

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

    //argparse4j converts names like "--http-timeout" to dests of "http_timeout"
    final int httpTimeout = parseTimeout(options, "http_timeout",
        HTTP_TIMEOUT_ENV_VAR, DEFAULT_HTTP_TIMEOUT_SECS);

    final int retryTimeout = parseTimeout(options, "retry_timeout",
        TOTAL_TIMEOUT_ENV_VAR, DEFAULT_TOTAL_TIMEOUT_SECS);

    log.debug("using HeliosClient httpTimeout={}, retryTimeout={}", httpTimeout, retryTimeout);

    return HeliosClient.newBuilder()
        .setEndpointSupplier(Endpoints.of(target.getEndpointSupplier()))
        .setHttpTimeout(httpTimeout, TimeUnit.SECONDS)
        .setRetryTimeout(retryTimeout, TimeUnit.SECONDS)
        .setSslHostnameVerification(!options.getBoolean("insecure"))
        .setUser(username)
        .build();
  }

  /**
   * Return the timeout value to use, first checking the argument provided to the CLI invocation,
   * then an environment variable, then the default value.
   */
  private static int parseTimeout(
      final Namespace options, final String dest,
      final String envVarName, final int defaultValue) {

    if (options.getInt(dest) != null) {
      return options.getInt(dest);
    }
    if (System.getenv(envVarName) != null) {
      // if this is not an integer then let it blow up
      return Integer.parseInt(System.getenv(envVarName));
    }
    return defaultValue;
  }

  public static boolean userConfirmed(final PrintStream out, final BufferedReader stdin)
      throws IOException {
    out.printf("Do you want to continue? [y/N]%n");

    final String line = stdin.readLine().trim();

    if (line.length() < 1) {
      return false;
    }
    final char c = line.charAt(0);

    if (c != 'Y' && c != 'y') {
      return false;
    }

    return true;
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

  public static <K extends Comparable<K>, V> void printMap(final PrintStream out, final String name,
                                                           final Function<V, String> transform,
                                                           final Map<K, V> values) {
    out.print(name);
    boolean first = true;
    for (final K key : Ordering.natural().sortedCopy(values.keySet())) {
      if (!first) {
        out.print(Strings.repeat(" ", name.length()));
      }
      final V value = values.get(key);
      out.printf("%s=%s%n", key, transform.apply(value));
      first = false;
    }
    if (first) {
      out.println();
    }
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

  // shared between JobStartCommand and JobStopCommand
  public static int setGoalOnHosts(final HeliosClient client, final PrintStream out,
                                   final boolean json, final List<String> hosts,
                                   final Deployment deployment, final String token)
      throws InterruptedException, ExecutionException {
    int code = 0;

    for (final String host : hosts) {
      if (!json) {
        out.printf("%s: ", host);
      }
      final SetGoalResponse result = client.setGoal(deployment, host, token).get();
      if (result.getStatus() == SetGoalResponse.Status.OK) {
        if (json) {
          out.print(result.toJsonString());
        } else {
          out.printf("done%n");
        }
      } else {
        if (json) {
          out.print(result.toJsonString());
        } else {
          out.printf("failed: %s%n", result);
        }
        code = 1;
      }
    }

    return code;
  }
}

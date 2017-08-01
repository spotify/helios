/*-
 * -\-\-
 * Helios System Tests
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

package com.spotify.helios.system;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.helios.Polling;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.junit.Test;

public class LabelTest extends SystemTestBase {

  /**
   * @param hosts Hostnames for which to get labels.
   *
   * @return a map whose keys are hostnames and values are maps of label key-vals.
   */
  private Callable<Map<String, Map<String, String>>> hostLabels(final Set<String> hosts) {
    return () -> {
      final ImmutableMap.Builder<String, Map<String, String>> hostLabels = ImmutableMap.builder();

      for (final String host : hosts) {
        final Map<String, HostStatus> status = Json.read(
            cli("hosts", host, "--json"),
            new TypeReference<Map<String, HostStatus>>() {
            });

        final HostStatus hostStatus = status.get(host);
        if (hostStatus == null) {
          return null;
        }

        final Map<String, String> labels = hostStatus.getLabels();
        if (labels != null && !labels.isEmpty()) {
          hostLabels.put(host, labels);
        } else {
          return null;
        }

      }
      return hostLabels.build();
    };
  }

  @Test
  public void testHostLabels() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost(), "--labels", "role=foo", "xyz=123");

    // Wait for the agent to report labels
    final Map<String, Map<String, String>> labels = Polling.await(
        LONG_WAIT_SECONDS, SECONDS, hostLabels(ImmutableSet.of(testHost())));

    final ImmutableMap<String, ImmutableMap<String, String>> expectedLabels =
        ImmutableMap.of(testHost(), ImmutableMap.of("role", "foo", "xyz", "123"));
    assertThat(labels, equalTo(expectedLabels));
  }

  @Test
  public void testCliHosts() throws Exception {
    final String testHost1 = testHost() + "1";
    final String testHost2 = testHost() + "2";

    startDefaultMaster();
    startDefaultAgent(testHost1, "--labels", "role=foo", "xyz=123");
    startDefaultAgent(testHost2, "--labels", "role=bar", "xyz=123");

    // Wait for the agents to report labels
    Polling.await(LONG_WAIT_SECONDS, SECONDS, hostLabels(ImmutableSet.of(testHost1, testHost2)));

    // Test all these host selectors in one test method to reduce testing time.
    // We can't start masters and agents in @BeforeClass because startDefaultMaster() isn't static
    final String fooHosts = cli("hosts", "-s", "role=foo");
    assertThat(fooHosts, containsString(testHost1));
    assertThat(fooHosts, not(containsString(testHost2)));

    final String xyzHosts = cli("hosts", "-s", "xyz=123");
    assertThat(xyzHosts, allOf(containsString(testHost1), containsString(testHost2)));

    final String fooAndXyzHosts = cli("hosts", "-s", "role=foo", "-s", "xyz=123");
    assertThat(fooAndXyzHosts, containsString(testHost1));
    assertThat(fooAndXyzHosts, not(containsString(testHost2)));

    final String notFooHosts = cli("hosts", "-s", "role!=foo");
    assertThat(notFooHosts, not(containsString(testHost1)));
    assertThat(notFooHosts, containsString(testHost2));

    final String inFooBarHosts = cli("hosts", "-s", "role in (foo, bar)");
    assertThat(inFooBarHosts, allOf(containsString(testHost1), containsString(testHost2)));

    final String notInFooBarHosts = cli("hosts", "-s", "role notin (foo, bar)");
    assertThat(notInFooBarHosts, not(anyOf(containsString(testHost1), containsString(testHost2))));

    final String nonHosts = cli("hosts", "-s", "role=doesnt_exist");
    assertThat(nonHosts, not(anyOf(containsString(testHost1), containsString(testHost2))));

    final String nonHosts2 = cli("hosts", "-s", "role=doesnt_exist", "-s", "xyz=123");
    assertThat(nonHosts2, not(anyOf(containsString(testHost1), containsString(testHost2))));
  }
}

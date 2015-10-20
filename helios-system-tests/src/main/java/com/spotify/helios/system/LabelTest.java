/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.Polling;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LabelTest extends SystemTestBase {

  @Test
  public void testHostStatus() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost(), "--labels", "role=foo", "xyz=123");
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Wait for the agent to report labels
    final Map<String, String> labels = Polling.await(LONG_WAIT_SECONDS, SECONDS,
       new Callable<Map<String, String>>() {
         @Override
         public Map<String, String> call()
             throws Exception {
           Map<String, HostStatus> status = Json.read(
               cli("hosts", testHost(), "--json"),
               new TypeReference<Map<String, HostStatus>>() {
               });
           final Map<String, String>
               labels =
               status.get(testHost()).getLabels();
           if (labels != null && !labels.isEmpty()) {
             return labels;
           } else {
             return null;
           }
         }
       });

    assertEquals(ImmutableMap.of("role", "foo", "xyz", "123"), labels);
  }

  @Test
  public void testCliHosts() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost(), "--labels", "role=foo", "xyz=123");
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Wait for the agent to report labels
    Polling.await(LONG_WAIT_SECONDS, SECONDS,
       new Callable<Map<String, String>>() {
         @Override
         public Map<String, String> call()
             throws Exception {
           Map<String, HostStatus> status = Json.read(
               cli("hosts", testHost(), "--json"),
               new TypeReference<Map<String, HostStatus>>() {
               });
           final Map<String, String>
               labels =
               status.get(testHost()).getLabels();
           if (labels != null && !labels.isEmpty()) {
             return labels;
           } else {
             return null;
           }
         }
       });

    assertThat(cli("hosts", "--labels", "role=foo"), containsString(testHost()));
    assertThat(cli("hosts", "--labels", "xyz=123"), containsString(testHost()));
    assertThat(cli("hosts", "--labels", "role=foo", "xyz=123"), containsString(testHost()));

    assertThat(cli("hosts", "--labels", "role=doesnt_exist"), not(containsString(testHost())));
    assertThat(cli("hosts", "--labels", "role=doesnt_exist", "xyz=123"),
               not(containsString(testHost())));
  }
}

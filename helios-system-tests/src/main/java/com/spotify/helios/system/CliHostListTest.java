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

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.spotify.helios.Polling;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
import java.util.Map;
import java.util.concurrent.Callable;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.junit.Before;
import org.junit.Test;

public class CliHostListTest extends SystemTestBase {

  private String hostname1;
  private String hostname2;

  @Before
  public void initialize() throws Exception {
    startDefaultMaster();

    // Wait for master to come up
    Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<String>() {
      @Override
      public String call() throws Exception {
        final String output = cli("masters");
        return output.contains(masterName()) ? output : null;
      }
    });

    hostname1 = testHost() + "a";
    hostname2 = testHost() + "b";

    startDefaultAgent(hostname1);
    final AgentMain agent2 = startDefaultAgent(hostname2);

    // Wait for both agents to come up
    awaitHostRegistered(hostname1, LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(hostname2, UP, LONG_WAIT_SECONDS, SECONDS);

    // Stop agent2
    agent2.stopAsync().awaitTerminated();
  }

  @Test
  public void testHostListJson() throws Exception {
    final String jsonOutput = cli("hosts", "-f", "--json");
    final Map<String, HostStatus> statuses =
        Json.readUnchecked(jsonOutput, new TypeReference<Map<String, HostStatus>>() {});
    final HeliosClient client = defaultClient();
    final Map<String, HostStatus> expectedStatuses =
        client.hostStatuses(ImmutableList.of(hostname1, hostname2)).get();
    assertThat(expectedStatuses, equalTo(statuses));
  }

  @Test
  public void testStatusFilter() throws Exception {
    String jsonOutput = cli("hosts", "-f", "--json", "--status", "UP");
    Map<String, HostStatus> statuses =
        Json.readUnchecked(jsonOutput, new TypeReference<Map<String, HostStatus>>() {
        });
    final HeliosClient client = defaultClient();
    Map<String, HostStatus> expectedStatuses =
        client.hostStatuses(ImmutableList.of(hostname1)).get();
    assertThat(expectedStatuses, equalTo(statuses));

    jsonOutput = cli("hosts", "-f", "--json", "--status", "DOWN");
    statuses = Json.readUnchecked(jsonOutput, new TypeReference<Map<String, HostStatus>>() {});
    expectedStatuses = client.hostStatuses(ImmutableList.of(hostname2)).get();
    assertThat(expectedStatuses, equalTo(statuses));
  }

  @Test(expected = ArgumentParserException.class)
  public void testInvalidStatus() throws Exception {
    cli("hosts", "-f", "--json", "--status", "up");
  }
}

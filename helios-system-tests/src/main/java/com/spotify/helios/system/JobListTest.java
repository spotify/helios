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

import static com.google.common.collect.Iterables.get;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.JobId;
import java.util.Map;
import java.util.Map.Entry;
import org.junit.Test;

public class JobListTest extends SystemTestBase {
  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>() {};
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Create job
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);

    // Test successful find
    // testJobName is of the form job_test_hexstring:vhexstring
    final String result1 = cli("jobs", "ob_", "--json");
    final Map<String, Object> resultObj1 = OBJECT_MAPPER.readValue(result1, MAP_TYPE);
    assertFalse(resultObj1.isEmpty());
    final Entry<String, Object> firstEntry = get(resultObj1.entrySet(), 0);
    assertEquals(jobId.toString(), firstEntry.getKey());

    // Test didn't find
    final String result2 = cli("jobs", "FramAZaMaWonTF1nD", "--json");
    final Map<String, Object> resultObj2 = OBJECT_MAPPER.readValue(result2, MAP_TYPE);
    // It might conceivably get here at some point, but better be empty if it does
    assertTrue(resultObj2.isEmpty());

    final String result3 = cli("jobs", "-y", "--json");
    final Map<String, Object> resultObj3 = OBJECT_MAPPER.readValue(result3, MAP_TYPE);
    assertTrue("Expected empty map but got: " + result3, resultObj3.isEmpty());

    final HeliosClient client = defaultClient();
    client.deploy(Deployment.of(jobId, Goal.START), testHost());
    awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);

    final String result4 = cli("jobs", "-y", "--json");
    final Map<String, Object> resultObj4 = OBJECT_MAPPER.readValue(result4, MAP_TYPE);
    assertFalse(resultObj4.isEmpty());
  }
}

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

package com.spotify.helios.system;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.helios.common.descriptors.JobId;

import org.junit.Test;

import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.collect.Iterables.get;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JobListTest extends SystemTestBase {
  private static final TypeReference<Map<String, Object>> MAP_TYPE =
      new TypeReference<Map<String, Object>>(){};
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    // Create job
    final JobId jobId = createJob(testJobName, testJobVersion, "busybox", IDLE_COMMAND);

    // Test successful find
    // testJobName is of the form job_test_hexstring:vhexstring
    final String result1 = cli("jobs", "ob_", "--json");
    final Map<String, Object> resultObj1 = OBJECT_MAPPER.readValue(result1, MAP_TYPE);
    assertFalse(resultObj1.isEmpty());
    final Entry<String, Object> firstEntry = get(resultObj1.entrySet(), 0);
    assertEquals(jobId.toString(), firstEntry.getKey());

    // Test didn't find
    final String result2 = cli("jobs", "FramAZaMaWonTF1nD", "--json");
    try {
      final Map<String, Object> resultObj2 = OBJECT_MAPPER.readValue(result2, MAP_TYPE);
      // It might conceivably get here at some point, but better be empty if it does
      assertTrue(resultObj2.isEmpty());
    } catch (JsonParseException e) {}
  }
}

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

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;

import org.junit.Test;

import java.util.Map;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

public class MultipleHostsTest extends SystemTestBase {
  @Test
  public void testHostStatuses() throws Exception {
    final String aHost = testHost() + "a";
    final String bHost = testHost() + "b";

    startDefaultMaster();
    startDefaultAgent(aHost);
    startDefaultAgent(bHost);
    awaitHostStatus(aHost, UP, LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(bHost, UP, LONG_WAIT_SECONDS, SECONDS);

    final Map<String, HostStatus> cliStatuses = new ObjectMapper().readValue(cli("hosts", "--json"),
        new TypeReference<Map<String, HostStatus>>(){});
    assertTrue("status must contain key for " + aHost, cliStatuses.containsKey(aHost));
    assertTrue("status must contain key for " + bHost, cliStatuses.containsKey(bHost));

    final HeliosClient client = defaultClient();
    final Map<String, HostStatus> clientStatuses = client.hostStatuses(
        ImmutableList.of(aHost, bHost)).get();

    assertTrue("status must contain key for " + aHost, clientStatuses.containsKey(aHost));
    assertTrue("status must contain key for " + bHost, clientStatuses.containsKey(bHost));
  }
}

/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.rollingupdate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostSelector;

import org.junit.Test;

import java.util.Map;

import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class HostMatcherTest {

  public static final Map<String, Map<String, String>> hostsAndLabels = ImmutableMap.of(
      "host3", (Map<String, String>) ImmutableMap.of("x", "y"),
      "host11", (Map<String, String>) ImmutableMap.of("foo", "bar"),
      "host2", (Map<String, String>) ImmutableMap.of("foo", "bar"),
      "host1", (Map<String, String>) ImmutableMap.of("foo", "bar")
  );

  @Test
  public void testHostMatcher() {
    final RollingUpdateService.HostMatcher hostMatcher = new
        RollingUpdateService.HostMatcher(hostsAndLabels);
    final DeploymentGroup deploymentGroup = DeploymentGroup.newBuilder()
        .setName("my_group")
        .setHostSelectors(Lists.newArrayList(
            HostSelector.parse("foo=bar")
        ))
        .build();

    assertArrayEquals(hostMatcher.getMatchingHosts(deploymentGroup).toArray(),
                      new String[]{"host1", "host2", "host11"});
  }

  @Test
  public void testDeploymentGroupWithNoSelectors() {
    final RollingUpdateService.HostMatcher hostMatcher = new
        RollingUpdateService.HostMatcher(hostsAndLabels);
    final DeploymentGroup deploymentGroup = DeploymentGroup.newBuilder()
        .setName("my_group")
        .setHostSelectors(EMPTY_LIST)
        .build();

    assertTrue(hostMatcher.getMatchingHosts(deploymentGroup).isEmpty());
  }

}
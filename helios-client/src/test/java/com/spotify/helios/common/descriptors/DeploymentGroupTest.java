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

package com.spotify.helios.common.descriptors;

import com.google.common.collect.ImmutableList;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DeploymentGroupTest {

  @Test
  public void verifyBuilder() {
    final DeploymentGroup.Builder builder = DeploymentGroup.newBuilder();

    // Input to setXXX
    final String setName = "foo-group";
    //noinspection ConstantConditions
    final List<HostSelector> setHostSelectors =
        ImmutableList.of(HostSelector.parse("foo=bar"), HostSelector.parse("baz=qux"));

    // Check setXXX methods
    builder.setName(setName);
    builder.setHostSelectors(setHostSelectors);

    assertEquals("name", setName, builder.getName());
    assertEquals("hostSelectors", setHostSelectors, builder.getHostSelectors());

    // Check final output
    final DeploymentGroup deploymentGroup = builder.build();
    assertEquals("name", setName, deploymentGroup.getName());
    assertEquals("hostSelectors", setHostSelectors, deploymentGroup.getHostSelectors());
  }
}

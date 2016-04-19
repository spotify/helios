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

public class RollingOperationTest {

  @Test
  public void verifyBuilder() {
    final RollingOperation.Builder builder = RollingOperation.newBuilder();

    // Input to setXXX
    final String setDeploymentGroupName = "foo-group";
    //noinspection ConstantConditions
    final List<HostSelector> setHostSelectors =
        ImmutableList.of(HostSelector.parse("foo=bar"), HostSelector.parse("baz=qux"));
    final JobId setJobId = JobId.fromString("foo:0.1.0");
    final RolloutOptions setRolloutOptions = RolloutOptions.newBuilder()
        .setTimeout(1000)
        .setParallelism(2)
        .setMigrate(false)
        .build();
    final RollingOperation.Reason setReason = RollingOperation.Reason.MANUAL;

    // Check setXXX methods
    builder.setDeploymentGroupName(setDeploymentGroupName);
    builder.setJobId(setJobId);
    builder.setRolloutOptions(setRolloutOptions);
    builder.setReason(setReason);

    assertEquals("name", setDeploymentGroupName, builder.getDeploymentGroupName());
    assertEquals("jobId", setJobId, builder.getJobId());
    assertEquals("rolloutOptions", setRolloutOptions, builder.getRolloutOptions());
    assertEquals("reason", setReason, builder.getReason());

    // Check final output
    final RollingOperation deploymentGroup = builder.build();
    assertEquals("name", setDeploymentGroupName, deploymentGroup.getDeploymentGroupName());
    assertEquals("jobId", setJobId, deploymentGroup.getJobId());
    assertEquals("rolloutOptions", setRolloutOptions, deploymentGroup.getRolloutOptions());
    assertEquals("reason", setReason, deploymentGroup.getReason());
  }
}

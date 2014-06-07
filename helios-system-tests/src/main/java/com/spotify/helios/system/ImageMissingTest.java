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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.ThrottleState.IMAGE_MISSING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class ImageMissingTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    JobId jobId = createJob(testJobName, testJobVersion, "this_sould_not_exist",
                            ImmutableList.of("/bin/true"));

    deployJob(jobId, testHost());
    awaitJobThrottle(client, testHost(), jobId, IMAGE_MISSING, LONG_WAIT_MINUTES, MINUTES);

    final HostStatus hostStatus = client.hostStatus(testHost()).get();
    final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
    assertEquals(TaskStatus.State.FAILED, taskStatus.getState());
  }
}

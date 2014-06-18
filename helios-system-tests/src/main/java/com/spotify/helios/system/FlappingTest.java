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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static com.spotify.helios.common.descriptors.ThrottleState.NO;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class FlappingTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, testHost(), UP, LONG_WAIT_MINUTES, MINUTES);

    final int flappingDurationSeconds = 30;
    final long epoch = MILLISECONDS.toSeconds(System.currentTimeMillis());
    final long recoveryEpoch = epoch + flappingDurationSeconds;

    final Job flapper = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(asList("sh", "-c", "while [ `date +%s` \\> \"" + recoveryEpoch + "\" ]; " +
                                       "do sleep 1; date +%s; done"))
        .build();

    // Verify that the job is classified as flapping
    JobId jobId = createJob(flapper);
    deployJob(jobId, testHost());
    awaitJobThrottle(client, testHost(), jobId, FLAPPING, LONG_WAIT_MINUTES, MINUTES);

    // Verify that the job recovers
    awaitJobThrottle(client, testHost(), jobId, NO, LONG_WAIT_MINUTES, MINUTES);
  }
}

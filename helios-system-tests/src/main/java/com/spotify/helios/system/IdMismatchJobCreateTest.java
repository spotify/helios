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

import static com.spotify.helios.common.descriptors.Job.DEFAULT_NETWORK_MODE;
import static com.spotify.helios.common.descriptors.Job.EMPTY_CAPS;
import static com.spotify.helios.common.descriptors.Job.EMPTY_CREATED;
import static com.spotify.helios.common.descriptors.Job.EMPTY_CREATING_USER;
import static com.spotify.helios.common.descriptors.Job.EMPTY_ENV;
import static com.spotify.helios.common.descriptors.Job.EMPTY_EXPIRES;
import static com.spotify.helios.common.descriptors.Job.EMPTY_GRACE_PERIOD;
import static com.spotify.helios.common.descriptors.Job.EMPTY_HEALTH_CHECK;
import static com.spotify.helios.common.descriptors.Job.EMPTY_HOSTNAME;
import static com.spotify.helios.common.descriptors.Job.EMPTY_LABELS;
import static com.spotify.helios.common.descriptors.Job.EMPTY_METADATA;
import static com.spotify.helios.common.descriptors.Job.EMPTY_PORTS;
import static com.spotify.helios.common.descriptors.Job.EMPTY_RAMDISKS;
import static com.spotify.helios.common.descriptors.Job.EMPTY_REGISTRATION;
import static com.spotify.helios.common.descriptors.Job.EMPTY_REGISTRATION_DOMAIN;
import static com.spotify.helios.common.descriptors.Job.EMPTY_RESOURCES;
import static com.spotify.helios.common.descriptors.Job.EMPTY_SECONDS_TO_WAIT;
import static com.spotify.helios.common.descriptors.Job.EMPTY_SECURITY_OPT;
import static com.spotify.helios.common.descriptors.Job.EMPTY_TOKEN;
import static com.spotify.helios.common.descriptors.Job.EMPTY_VOLUMES;
import static org.junit.Assert.assertEquals;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.CreateJobResponse;
import org.junit.Test;

public class IdMismatchJobCreateTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    final HeliosClient client = defaultClient();

    final CreateJobResponse createIdMismatch = client.createJob(
        new Job(JobId.fromString("bad:job:deadbeef"), BUSYBOX, EMPTY_HOSTNAME, EMPTY_CREATED,
            IDLE_COMMAND, EMPTY_ENV, EMPTY_RESOURCES, EMPTY_PORTS, EMPTY_REGISTRATION,
            EMPTY_GRACE_PERIOD, EMPTY_VOLUMES, EMPTY_EXPIRES,
            EMPTY_REGISTRATION_DOMAIN, EMPTY_CREATING_USER, EMPTY_TOKEN,
            EMPTY_HEALTH_CHECK, EMPTY_SECURITY_OPT, DEFAULT_NETWORK_MODE,
            EMPTY_METADATA, EMPTY_CAPS, EMPTY_CAPS, EMPTY_LABELS, EMPTY_SECONDS_TO_WAIT,
            EMPTY_RAMDISKS)
    ).get();

    // TODO (dano): Maybe this should be ID_MISMATCH but then JobValidator must become able to
    // TODO (dano): communicate that
    assertEquals(CreateJobResponse.Status.INVALID_JOB_DEFINITION, createIdMismatch.getStatus());
  }
}

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
import com.spotify.helios.common.protocol.CreateJobResponse;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class IdMismatchJobCreateTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    final HeliosClient client = defaultClient();

    final CreateJobResponse createIdMismatch = client.createJob(
        new Job(JobId.fromString("bad:job:deadbeef"), "busyBox", DO_NOTHING_COMMAND, EMPTY_ENV,
                EMPTY_PORTS, EMPTY_REGISTRATION)).get();

    // TODO (dano): Maybe this should be ID_MISMATCH but then JobValidator must become able to communicate that
    assertEquals(CreateJobResponse.Status.INVALID_JOB_DEFINITION, createIdMismatch.getStatus());
  }
}

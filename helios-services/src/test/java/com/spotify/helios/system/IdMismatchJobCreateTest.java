/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.helios.common.HeliosClient;
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

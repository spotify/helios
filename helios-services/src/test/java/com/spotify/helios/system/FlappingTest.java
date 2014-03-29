/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static java.util.concurrent.TimeUnit.MINUTES;

public class FlappingTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_HOST);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", ImmutableList.of("/bin/true"));
    deployJob(jobId, TEST_HOST);
    awaitJobThrottle(client, TEST_HOST, jobId, FLAPPING, LONG_WAIT_MINUTES, MINUTES);
  }
}

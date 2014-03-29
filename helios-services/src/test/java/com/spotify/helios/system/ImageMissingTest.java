/**
 * Copyright (C) 2014 Spotify AB
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
    startDefaultAgent(TEST_HOST);

    final HeliosClient client = defaultClient();

    awaitHostStatus(client, TEST_HOST, UP, LONG_WAIT_MINUTES, MINUTES);

    JobId jobId = createJob(JOB_NAME, JOB_VERSION, "this_sould_not_exist",
                            ImmutableList.of("/bin/true"));

    deployJob(jobId, TEST_HOST);
    awaitJobThrottle(client, TEST_HOST, jobId, IMAGE_MISSING, LONG_WAIT_MINUTES, MINUTES);

    final HostStatus hostStatus = client.hostStatus(TEST_HOST).get();
    final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
    assertEquals(TaskStatus.State.FAILED, taskStatus.getState());
  }

}

/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static com.spotify.helios.common.descriptors.ThrottleState.IMAGE_MISSING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class ImageMissingTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_AGENT);

    final Client client = defaultClient();

    awaitAgentStatus(client, TEST_AGENT, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    JobId jobId = createJob(JOB_NAME, JOB_VERSION, "this_sould_not_exist",
                            ImmutableList.of("/bin/true"));

    deployJob(jobId, TEST_AGENT);
    awaitJobThrottle(client, TEST_AGENT, jobId, IMAGE_MISSING, WAIT_TIMEOUT_SECONDS, SECONDS);

    final AgentStatus agentStatus = client.agentStatus(TEST_AGENT).get();
    final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
    assertEquals(TaskStatus.State.FAILED, taskStatus.getState());
  }

}

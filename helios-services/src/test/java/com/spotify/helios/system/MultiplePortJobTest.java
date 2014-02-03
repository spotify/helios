/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;

import org.junit.Test;

import java.util.Map;

import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertNotNull;

public class MultiplePortJobTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_AGENT);

    final HeliosClient client = defaultClient();

    awaitAgentStatus(client, TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    final Map<String, PortMapping> ports = ImmutableMap.of("foo", PortMapping.of(4711),
                                                           "bar", PortMapping.of(EXTERNAL_PORT));

    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", DO_NOTHING_COMMAND, EMPTY_ENV,
                                  ports);
    assertNotNull(jobId);
    deployJob(jobId, TEST_AGENT);
    awaitJobState(client, TEST_AGENT, jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);
  }

}

/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class JobHistoryTest extends SystemTestBase {
  @Test
  public void testJobHistory() throws Exception {
    startDefaultMaster();
    final HeliosClient client = defaultClient();

    startDefaultAgent(getTestHost());
    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "ubuntu:12.04", ImmutableList.of("/bin/true"));
    deployJob(jobId, getTestHost());
    awaitJobState(client, getTestHost(), jobId, EXITED, LONG_WAIT_MINUTES, MINUTES);
    undeployJob(jobId, getTestHost());
    final TaskStatusEvents events = Polling.await(
        WAIT_TIMEOUT_SECONDS, SECONDS, new Callable<TaskStatusEvents>() {
      @Override
      public TaskStatusEvents call() throws Exception {
        final TaskStatusEvents events = client.jobHistory(jobId).get();
        if (events.getEvents().size() < 5) {
          return null;
        }
        return events;
      }
    });
    final List<TaskStatusEvent> eventsList = events.getEvents();

    final TaskStatusEvent event0 = eventsList.get(0);
    assertEquals(State.PULLING_IMAGE, event0.getStatus().getState());
    assertNull(event0.getStatus().getContainerId());

    final TaskStatusEvent event1 = eventsList.get(1);
    assertEquals(State.CREATING, event1.getStatus().getState());
    assertNull(event1.getStatus().getContainerId());

    final TaskStatusEvent event2 = eventsList.get(2);
    assertEquals(State.STARTING, event2.getStatus().getState());
    assertNotNull(event2.getStatus().getContainerId());

    final TaskStatusEvent event3 = eventsList.get(3);
    assertEquals(State.RUNNING, event3.getStatus().getState());

    final TaskStatusEvent event4 = eventsList.get(4);
    assertEquals(State.EXITED, event4.getStatus().getState());
  }
}

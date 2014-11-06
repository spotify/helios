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

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus.Status;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;

import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JobHistoryTest extends SystemTestBase {
  @Test
  public void testJobHistory() throws Exception {
    startDefaultMaster();
    final HeliosClient client = defaultClient();

    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), Status.UP, LONG_WAIT_SECONDS, SECONDS);
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    deployJob(jobId, testHost());
    awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);
    undeployJob(jobId, testHost());
    awaitTaskGone(client, testHost(), jobId, LONG_WAIT_SECONDS, SECONDS);
    final TaskStatusEvents events = Polling.await(
        WAIT_TIMEOUT_SECONDS, SECONDS, new Callable<TaskStatusEvents>() {
      @Override
      public TaskStatusEvents call() throws Exception {
        final TaskStatusEvents events = client.jobHistory(jobId).get();
        final int size = events.getEvents().size();
        if (size == 0) {
          return null;
        }
        // We sometimes get more than one PULLING_IMAGE in the history if a pull tempfails.
        int requiredEventCount = -1;
        for (int i = 0; i < size; i++) {
          if (events.getEvents().get(i).getStatus().getState() != State.PULLING_IMAGE) {
            requiredEventCount = i + 4;
            break;
          }
        }

        if (requiredEventCount == -1) {
          return null;
        }

        if (size < requiredEventCount) {
          return null;
        }
        return events;
      }
    });
    final List<TaskStatusEvent> eventsList = events.getEvents();
    int n = 0;

    while (true) {
      final TaskStatusEvent event = eventsList.get(n);
      if (event.getStatus().getState() != State.PULLING_IMAGE) {
        break;
      }
      assertNull(event.getStatus().getContainerId());
      n++;
    }
    final TaskStatusEvent event1 = eventsList.get(n);
    assertEquals(State.CREATING, event1.getStatus().getState());
    assertNull(event1.getStatus().getContainerId());

    final TaskStatusEvent event2 = eventsList.get(n + 1);
    assertEquals(State.STARTING, event2.getStatus().getState());
    assertNotNull(event2.getStatus().getContainerId());

    final TaskStatusEvent event3 = eventsList.get(n + 2);
    assertEquals(State.RUNNING, event3.getStatus().getState());

    final TaskStatusEvent event4 = eventsList.get(n + 3);
    final State finalState = event4.getStatus().getState();
    assertTrue(finalState == State.EXITED || finalState == State.STOPPED);
  }
}

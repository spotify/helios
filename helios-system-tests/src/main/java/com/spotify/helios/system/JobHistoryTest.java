/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus.Status;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.Arrays;
import java.util.ListIterator;
import java.util.concurrent.Callable;

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
            requiredEventCount = i + 6;
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
    final ListIterator<TaskStatusEvent> it = events.getEvents().listIterator();

    while (true) {
      final TaskStatusEvent event = it.next();
      if (event.getStatus().getState() != State.PULLING_IMAGE) {
        //rewind so that this event is the one returned by the next call to it.next() below
        it.previous();
        break;
      }
      assertThat(event, not(hasContainerId()));
    }

    assertThat(it.next(), allOf(hasState(State.PULLED_IMAGE), not(hasContainerId())));

    assertThat(it.next(), allOf(hasState(State.CREATING), not(hasContainerId())));

    assertThat(it.next(), allOf(hasState(State.STARTING), hasContainerId()));

    assertThat(it.next(), hasState(State.RUNNING));

    assertThat(it.next(), hasState(State.STOPPING));

    assertThat(it.next(), hasState(State.EXITED, State.STOPPED));
  }

  @Test
  public void testJobHistoryDisabled() throws Exception {
    startDefaultMaster();
    final HeliosClient client = defaultClient();

    startDefaultAgent(testHost(), "--disable-job-history");
    awaitHostStatus(testHost(), Status.UP, LONG_WAIT_SECONDS, SECONDS);
    final JobId jobId = createJob(testJobName, testJobVersion, BUSYBOX, IDLE_COMMAND);
    deployJob(jobId, testHost());
    awaitJobState(client, testHost(), jobId, RUNNING, LONG_WAIT_SECONDS, SECONDS);
    undeployJob(jobId, testHost());
    awaitTaskGone(client, testHost(), jobId, LONG_WAIT_SECONDS, SECONDS);
  }

  private static Matcher<TaskStatusEvent> hasState(final State... possibleStates) {
    final String description =
        "TaskStatusEvent with status.state in " + Arrays.toString(possibleStates);

    return new CustomTypeSafeMatcher<TaskStatusEvent>(description) {
      @Override
      protected boolean matchesSafely(final TaskStatusEvent event) {
        final State actual = event.getStatus().getState();
        for (final State state : possibleStates) {
          if (state == actual) {
            return true;
          }
        }
        return false;
      }
    };
  }

  private static Matcher<TaskStatusEvent> hasContainerId() {
    return new CustomTypeSafeMatcher<TaskStatusEvent>("a non-null status.containerId") {
      @Override
      protected boolean matchesSafely(final TaskStatusEvent item) {
        return item.getStatus().getContainerId() != null;
      }
    };
  }

}

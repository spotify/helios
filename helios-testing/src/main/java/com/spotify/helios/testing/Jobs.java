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

package com.spotify.helios.testing;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

// TODO (dxia) Does this class make sense? It's just a collection of static methods.
class Jobs {

  private static final Logger log = LoggerFactory.getLogger(Jobs.class);

  static final long TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);

  static <T> T get(final ListenableFuture<T> future)
      throws InterruptedException, ExecutionException, TimeoutException {
    return get(future, TIMEOUT_MILLIS);
  }

  static <T> T get(final ListenableFuture<T> future, final long timeout)
      throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(timeout, MILLISECONDS);
  }

  static String getJobDescription(final Job job) {
    final String shortHash = job.getId().getHash().substring(0, 7);
    return String.format("%s (Job %s)", job.getImage(), shortHash);
  }

  /**
   * Undeploy the job from all specified hosts, and delete the job. Any failures will be ignored,
   * and we will keep trying each host. A list of errors encountered along the way will be returned
   * to the caller.
   * @param client the HeliosClient to use
   * @param job the job to undeploy and delete
   * @param hosts the hosts to undeploy from
   * @param errors errors encountered during the undeploy will be added to this list
   * @return the list of errors
   */
  static List<AssertionError> undeploy(final HeliosClient client, final Job job,
                                       final List<String> hosts,
                                       final List<AssertionError> errors) {
    final JobId id = job.getId();
    for (final String host : hosts) {
      log.info("Undeploying {} from {}", getJobDescription(job), host);
      final JobUndeployResponse response;
      try {
        response = get(client.undeploy(id, host));
        if (response.getStatus() != JobUndeployResponse.Status.OK &&
            response.getStatus() != JobUndeployResponse.Status.JOB_NOT_FOUND) {
          errors.add(new AssertionError(format("Failed to undeploy job %s - %s",
                                               id, response)));
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        errors.add(new AssertionError(e));
      }
    }

    try {
      log.debug("Deleting job {}", id);
      final JobDeleteResponse response = get(client.deleteJob(id));
      if (response.getStatus() != JobDeleteResponse.Status.OK &&
          response.getStatus() != JobDeleteResponse.Status.JOB_NOT_FOUND) {
        errors.add(new AssertionError(format("Failed to delete job %s - %s",
                                             id.toString(), response.toString())));
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      errors.add(new AssertionError(e));
    }

    return errors;
  }

  // TODO(staffan): This is probably not the right place for these methods. And they likely need
  // some refactoring.
  public static void verifyHealthy(final Job job, final HeliosClient client) throws AssertionError {
    log.debug("Checking health of {}", job.getImage());
    final JobStatus status = Futures.getUnchecked(client.jobStatus(job.getId()));
    if (status == null) {
      return;
    }
    for (final Map.Entry<String, TaskStatus> entry : status.getTaskStatuses().entrySet()) {
      verifyHealthy(job, client, entry.getKey(), entry.getValue());
    }
  }

  // TODO(staffan): This is probably not the right place for these methods. And they likely need
  // some refactoring.
  public static void verifyHealthy(final Job job, final HeliosClient client,
                                   final String host, final TaskStatus status) {
    log.debug("Checking health of {} on {}", job.getImage(), host);
    final TaskStatus.State state = status.getState();
    if (state == TaskStatus.State.FAILED ||
        state == TaskStatus.State.EXITED ||
        state == TaskStatus.State.STOPPED) {
      // Throw exception which should stop the test dead in it's tracks
      String stateString = state.toString();
      if (status.getThrottled() != ThrottleState.NO) {
        stateString += format("(%s)", status.getThrottled());
      }
      throw new AssertionError(format(
          "Unexpected job state %s for job %s with image %s on host %s. Check helios agent "
          + "logs for details. If you're using HeliosSoloDeployment, set "
          + "`HeliosSoloDeployment.fromEnv().removeHeliosSoloOnExit(false)` and check the"
          + "logs of the helios-solo container with `docker logs <container ID>`.",
          stateString, job.getId().toShortString(), job.getImage(), host));
    }
  }
}

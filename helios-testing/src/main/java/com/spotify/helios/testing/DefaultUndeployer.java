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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.helios.testing.Jobs.get;
import static com.spotify.helios.testing.Jobs.getJobDescription;
import static java.lang.String.format;

class DefaultUndeployer implements Undeployer {

  private static final Logger log = LoggerFactory.getLogger(DefaultUndeployer.class);

  private final HeliosClient client;

  DefaultUndeployer(final HeliosClient client) {
    this.client = checkNotNull(client, "client");
  }

  @Override
  public List<AssertionError> undeploy(final Job job, final List<String> hosts) {
    final List<AssertionError> errors = new ArrayList<>();
    final JobId id = job.getId();

    for (final String host : hosts) {
      log.info("Undeploying {} from {}", getJobDescription(job), host);
      final JobUndeployResponse response;
      try {
        response = get(client.undeploy(id, host));
        if (response.getStatus() != JobUndeployResponse.Status.OK &&
            response.getStatus() != JobUndeployResponse.Status.JOB_NOT_FOUND) {
          errors.add(new AssertionError(format("Failed to undeploy job %s - %s", id, response)));
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

}

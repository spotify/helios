/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.master.resources;

import static com.spotify.helios.common.protocol.TaskStatusEvents.Status.JOB_ID_NOT_FOUND;
import static com.spotify.helios.common.protocol.TaskStatusEvents.Status.OK;
import static com.spotify.helios.master.http.Responses.badRequest;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableList;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import java.util.List;
import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

@Path("/history")
public class HistoryResource {
  private final MasterModel model;
  private final MasterMetrics metrics;

  public HistoryResource(MasterModel model, final MasterMetrics metrics) {
    this.model = model;
    this.metrics = metrics;
  }

  /**
   * Returns the {@link TaskStatusEvents} for the specified job.
   *
   * @param jobId The ID of the job.
   *
   * @return The history of the jobs.
   *
   * @throws HeliosException If an unexpected error occurs.
   */
  @GET
  @Produces(APPLICATION_JSON)
  @Path("jobs/{id}")
  @Timed
  @ExceptionMetered
  public TaskStatusEvents jobHistory(@PathParam("id") @Valid final JobId jobId)
      throws HeliosException {
    if (!jobId.isFullyQualified()) {
      throw badRequest("Invalid id");
    }
    try {
      final List<TaskStatusEvent> events = model.getJobHistory(jobId);
      metrics.jobsHistoryEventSize(events.size());
      final TaskStatusEvents result = new TaskStatusEvents(events, OK);
      return result;
    } catch (JobDoesNotExistException e) {
      return new TaskStatusEvents(ImmutableList.<TaskStatusEvent>of(), JOB_ID_NOT_FOUND);
    }
  }
}

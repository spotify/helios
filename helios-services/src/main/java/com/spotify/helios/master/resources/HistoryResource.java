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

package com.spotify.helios.master.resources;

import com.google.common.collect.ImmutableList;

import javax.ws.rs.POST;
import javax.ws.rs.GET;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.common.protocol.ListenerResponse;
import com.spotify.helios.common.protocol.TaskStatusEvents;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;

import java.io.IOException;
import java.util.List;

import javax.validation.Valid;

import static com.spotify.helios.common.protocol.TaskStatusEvents.Status.JOB_ID_NOT_FOUND;
import static com.spotify.helios.common.protocol.TaskStatusEvents.Status.OK;
import static com.spotify.helios.master.http.Responses.badRequest;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

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

  @POST
  @Path("listeners")
  @Produces(APPLICATION_JSON)
  @Consumes(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public ListenerResponse listeners(@Valid final String listenerUrl) {
    try {
        model.addListener(Json.read(listenerUrl, String.class));
    } catch (HeliosRuntimeException e) {
        return new ListenerResponse(ListenerResponse.Status.DUPLICATE_LISTENER, listenerUrl);
    } catch (IOException e) {
        return new ListenerResponse(ListenerResponse.Status.INVALID_LISTENER, listenerUrl);
    }

    return new ListenerResponse(ListenerResponse.Status.OK, listenerUrl);
  }
}

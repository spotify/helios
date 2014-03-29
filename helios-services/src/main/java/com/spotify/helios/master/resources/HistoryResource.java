package com.spotify.helios.master.resources;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobIdParseException;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import com.yammer.metrics.annotation.ExceptionMetered;
import com.yammer.metrics.annotation.Timed;

import java.util.List;

import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import static com.spotify.helios.master.http.Responses.badRequest;
import static com.spotify.helios.master.http.Responses.notFound;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/history")
public class HistoryResource {

  private final MasterModel model;
  private final MasterMetrics metrics;

  public HistoryResource(final MasterModel model, final MasterMetrics metrics) {
    this.model = model;
    this.metrics = metrics;
  }

  @GET
  @Path("/jobs/{id}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public TaskStatusEvents jobHistoryGet(@PathParam("id") @Valid final JobId jobId)
      throws HeliosException, JobIdParseException, JsonProcessingException {
    if (!jobId.isFullyQualified()) {
      throw badRequest("Invalid id");
    }
    try {
      List<TaskStatusEvent> history = model.getJobHistory(jobId);
      metrics.jobsHistoryEventSize(history.size());
      return new TaskStatusEvents(history, TaskStatusEvents.Status.OK);
    } catch (JobDoesNotExistException e) {
      throw notFound(new TaskStatusEvents(ImmutableList.<TaskStatusEvent>of(),
                                          TaskStatusEvents.Status.JOB_ID_NOT_FOUND));
    }
  }
}

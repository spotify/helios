package com.spotify.helios.master;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;

import java.util.List;

import javax.validation.Valid;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import static com.spotify.helios.common.protocol.TaskStatusEvents.Status.JOB_ID_NOT_FOUND;
import static com.spotify.helios.common.protocol.TaskStatusEvents.Status.OK;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/history")
public class HistoryResource {
  private final MasterModel model;

  public HistoryResource(MasterModel model) {
    this.model = model;
  }

  @GET
  @Produces(APPLICATION_JSON)
  @Path("jobs/{id}")
  public TaskStatusEvents jobHistory(@PathParam("id") @Valid final JobId jobId) throws HeliosException {
    try {
      final List<TaskStatusEvent> events = model.getJobHistory(jobId);
      final TaskStatusEvents result = new TaskStatusEvents(events, OK);
      return result;
    } catch (JobDoesNotExistException e) {
      return new TaskStatusEvents(ImmutableList.<TaskStatusEvent>of(), JOB_ID_NOT_FOUND);
    }
  }
}

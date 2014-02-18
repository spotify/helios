package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.yammer.metrics.annotation.Timed;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/helios/taskstatus")
@Produces(MediaType.APPLICATION_JSON)
public class AgentModelTaskStatusResource {
  private final ZooKeeperAgentModel model;

  public AgentModelTaskStatusResource(final ZooKeeperAgentModel model) {
    this.model = model;
  }

  @GET
  @Timed
  public Map<JobId, TaskStatus> getTaskStatus() {
    return model.getTaskStatuses();
  }
}

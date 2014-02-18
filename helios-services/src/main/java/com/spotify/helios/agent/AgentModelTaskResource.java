package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import com.yammer.metrics.annotation.Timed;

import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

// Would be /tasks, but dropwizard uses /tasks/.
@Path("/helios/tasks")
@Produces(MediaType.APPLICATION_JSON)
public class AgentModelTaskResource {
  private final ZooKeeperAgentModel model;

  public AgentModelTaskResource(final ZooKeeperAgentModel model) {
    this.model = model;
  }

  @GET
  @Timed
  public Map<JobId, Task> getTasks() {
    return model.getTasks();
  }
}

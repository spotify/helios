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

package com.spotify.helios.agent;

import com.codahale.metrics.annotation.Timed;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.Task;
import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

// Would be /tasks, but dropwizard uses /tasks/.

/**
 * Makes it so you can view the tasks as the Agent sees things.
 */
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

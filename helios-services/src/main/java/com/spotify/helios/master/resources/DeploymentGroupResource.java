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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.Lists;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateDeploymentGroupResponse;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;
import com.spotify.helios.common.protocol.RemoveDeploymentGroupResponse;
import com.spotify.helios.common.protocol.RollingUpdateRequest;
import com.spotify.helios.common.protocol.RollingUpdateResponse;
import com.spotify.helios.master.DeploymentGroupDoesNotExistException;
import com.spotify.helios.master.DeploymentGroupExistsException;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.MasterModel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.validation.Valid;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/deployment-group")
public class DeploymentGroupResource {

  private final MasterModel model;

  private static final CreateDeploymentGroupResponse CREATED_RESPONSE =
      new CreateDeploymentGroupResponse(CreateDeploymentGroupResponse.Status.CREATED);
  private static final CreateDeploymentGroupResponse NOT_MODIFIED_RESPONSE =
      new CreateDeploymentGroupResponse(CreateDeploymentGroupResponse.Status.NOT_MODIFIED);
  private static final CreateDeploymentGroupResponse DEPLOYMENT_GROUP_ALREADY_EXISTS_RESPONSE =
      new CreateDeploymentGroupResponse(CreateDeploymentGroupResponse.Status.CONFLICT);

  public DeploymentGroupResource(final MasterModel model) {
    this.model = model;
  }

  @POST
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response createDeploymentGroup(@Valid final DeploymentGroup deploymentGroup) {
    try {
      model.addDeploymentGroup(deploymentGroup);
      return Response.ok(CREATED_RESPONSE).build();
    } catch (DeploymentGroupExistsException ignored) {
      final DeploymentGroup existing;
      try {
        existing = model.getDeploymentGroup(deploymentGroup.getName());
      } catch (DeploymentGroupDoesNotExistException e) {
        // Edge condition: There's a race where someone can potentially remove the deployment-group
        // while this operation is in progress. This should be very rare. If it does happen,
        // return 500.
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
      }

      if (!Objects.equals(existing.getHostSelectors(), deploymentGroup.getHostSelectors())) {
        return Response.ok(DEPLOYMENT_GROUP_ALREADY_EXISTS_RESPONSE).build();
      }

      return Response.ok(NOT_MODIFIED_RESPONSE).build();
    }
  }

  @GET
  @Path("/{name}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response getDeploymentGroup(@PathParam("name") final String name) {
    try {
      return Response.ok(model.getDeploymentGroup(name)).build();
    } catch (final DeploymentGroupDoesNotExistException e) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }

  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public List<String> getDeploymentGroup() {
    final List<String> deploymentGroups = Lists.newArrayList(model.getDeploymentGroups().keySet());
    Collections.sort(deploymentGroups);
    return deploymentGroups;
  }

  @DELETE
  @Path("/{name}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response removeDeploymentGroup(@PathParam("name") @Valid final String name) {
    try {
      model.removeDeploymentGroup(name);
      return Response.ok(new RemoveDeploymentGroupResponse(
          RemoveDeploymentGroupResponse.Status.REMOVED)).build();
    } catch (final DeploymentGroupDoesNotExistException e) {
      return Response.ok(new RemoveDeploymentGroupResponse(
          RemoveDeploymentGroupResponse.Status.DEPLOYMENT_GROUP_NOT_FOUND)).build();
    }
  }

  @POST
  @Path("/{name}/rolling-update")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response rollingUpdate(@PathParam("name") @Valid final String name,
                                @Valid final RollingUpdateRequest args) {
    try {
      final DeploymentGroup deploymentGroup = model.getDeploymentGroup(name);
      model.rollingUpdate(deploymentGroup, args.getJob(), args.getRolloutOptions());
      return Response.ok(new RollingUpdateResponse(RollingUpdateResponse.Status.OK)).build();
    } catch (DeploymentGroupDoesNotExistException e) {
      return Response.ok(new RollingUpdateResponse(
          RollingUpdateResponse.Status.DEPLOYMENT_GROUP_NOT_FOUND)).build();
    } catch (JobDoesNotExistException e) {
      return Response.ok(new RollingUpdateResponse(
          RollingUpdateResponse.Status.JOB_NOT_FOUND)).build();
    }
  }

  @POST
  @Path("/{name}/stop")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response stopDeploymentGroup(@PathParam("name") @Valid final String name) {
    try {
      model.stopDeploymentGroup(name);
      return Response.noContent().build();
    } catch (DeploymentGroupDoesNotExistException e) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }

  @GET
  @Path("/{name}/status")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response getDeploymentGroupStatus(@PathParam("name") @Valid final String name) {
    try {
      final DeploymentGroup deploymentGroup = model.getDeploymentGroup(name);
      final DeploymentGroupStatus deploymentGroupStatus = model.getDeploymentGroupStatus(name);

      final List<String> hosts = model.getDeploymentGroupHosts(name);

      final List<DeploymentGroupStatusResponse.HostStatus> result = Lists.newArrayList();

      for (final String host : hosts) {
        final HostStatus hostStatus = model.getHostStatus(host);
        JobId deployedJobId = null;
        TaskStatus.State state = null;

        if (hostStatus != null && hostStatus.getStatus().equals(HostStatus.Status.UP)) {
          for (final Map.Entry<JobId, Deployment> entry : hostStatus.getJobs().entrySet()) {
            if (name.equals(entry.getValue().getDeploymentGroupName())) {
              deployedJobId = entry.getKey();
              final TaskStatus taskStatus = hostStatus.getStatuses().get(deployedJobId);
              if (taskStatus != null) {
                state = taskStatus.getState();
              }
              break;
            }
          }

          result.add(new DeploymentGroupStatusResponse.HostStatus(host, deployedJobId, state));
        }
      }

      final DeploymentGroupStatusResponse.Status status;
      if (deploymentGroupStatus == null) {
        status = DeploymentGroupStatusResponse.Status.IDLE;
      } else if (deploymentGroupStatus.getState() == DeploymentGroupStatus.State.FAILED) {
        status = DeploymentGroupStatusResponse.Status.FAILED;
      } else if (deploymentGroupStatus.getState() == DeploymentGroupStatus.State.ROLLING_OUT) {
        status = DeploymentGroupStatusResponse.Status.ROLLING_OUT;
      } else {
        status = DeploymentGroupStatusResponse.Status.ACTIVE;
      }

      final String error = deploymentGroupStatus == null ? "" : deploymentGroupStatus.getError();
      return Response.ok(new DeploymentGroupStatusResponse(
          deploymentGroup, status, error, result, deploymentGroupStatus)).build();
    } catch (final DeploymentGroupDoesNotExistException e) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}

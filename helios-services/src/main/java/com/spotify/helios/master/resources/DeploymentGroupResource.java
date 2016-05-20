/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.master.resources;

import com.google.common.collect.Lists;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RollingOperation;
import com.spotify.helios.common.descriptors.RollingOperationStatus;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateDeploymentGroupResponse;
import com.spotify.helios.common.protocol.DeploymentGroupResponse;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;
import com.spotify.helios.common.protocol.RemoveDeploymentGroupResponse;
import com.spotify.helios.common.protocol.RollingUpdateRequest;
import com.spotify.helios.common.protocol.RollingUpdateResponse;
import com.spotify.helios.master.DeploymentGroupDoesNotExistException;
import com.spotify.helios.master.DeploymentGroupExistsException;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.master.RollingOperationDoesNotExistException;

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

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

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
      final DeploymentGroup deploymentGroup = model.getDeploymentGroup(name);
      final RollingOperation lastOp = model.getLastRollingOperation(name);

      final DeploymentGroupResponse.Builder response = DeploymentGroupResponse.builder()
          .name(deploymentGroup.getName())
          .hostSelectors(deploymentGroup.getHostSelectors());

      if (lastOp != null) {
        response.jobId(lastOp.getJobId());
        response.rolloutOptions(lastOp.getRolloutOptions());
      }

      return Response.ok(response.build()).build();
    } catch (final DeploymentGroupDoesNotExistException e) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
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

  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public List<String> getDeploymentGroup() {
    final List<String> deploymentGroups = Lists.newArrayList(model.getDeploymentGroups().keySet());
    Collections.sort(deploymentGroups);
    return deploymentGroups;
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

  private List<DeploymentGroupStatusResponse.HostStatus> hostStatuses(final String name)
    throws DeploymentGroupDoesNotExistException {
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
    return result;
  }

  @GET
  @Path("/{name}/status")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response getDeploymentGroupStatus(@PathParam("name") @Valid final String name) {
    // Respond with a 'deployment group status' inferred from the last rolling operation (if any)
    // in order to maintain API compatibility with older versions of Helios in which deployment
    // groups (as opposed to rolling operations) could be in states such as 'failed', or 'rolling
    // out'.
    try {
      final DeploymentGroupResponse.Builder dgResponse =
          DeploymentGroupResponse.builder()
              .name(name)
              .hostSelectors(model.getDeploymentGroup(name).getHostSelectors());

      final DeploymentGroupStatus status = model.getDeploymentGroupStatus(name);
      final DeploymentGroupStatusResponse.Builder response = DeploymentGroupStatusResponse.builder()
          .error("")
          .hostStatuses(hostStatuses(name))
          .deploymentGroupStatus(status);

      final RollingOperation lastOp = model.getLastRollingOperation(name);

      if (lastOp == null) {
        // No rolling operations have been recorded yet, so we're idle.
        response.status(DeploymentGroupStatusResponse.Status.IDLE);
        // Get the error from the deployment group, in case we set an error when it was stopped.
        response.error(status.getError());
        response.deploymentGroupResponse(dgResponse.build());
        return Response.ok(response.build()).build();
      }

      dgResponse.jobId(lastOp.getJobId());
      dgResponse.rolloutOptions(lastOp.getRolloutOptions());
      response.deploymentGroupResponse(dgResponse.build());

      final RollingOperationStatus lastOpStatus = model.getRollingOperationStatus(lastOp.getId());
      response.lastRollingOpStatus(lastOpStatus);
      switch (lastOpStatus.getState()) {
        case NEW:
          response.status(DeploymentGroupStatusResponse.Status.IDLE);
          break;
        case FAILED:
          response.status(DeploymentGroupStatusResponse.Status.FAILED);
          response.error(lastOpStatus.getError());
          break;
        case ROLLING_OUT:
          response.status(DeploymentGroupStatusResponse.Status.ROLLING_OUT);
          break;
        default:
          response.status(DeploymentGroupStatusResponse.Status.ACTIVE);
          break;
      }

      return Response.ok(response.build()).build();
    } catch (final DeploymentGroupDoesNotExistException | RollingOperationDoesNotExistException e) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}

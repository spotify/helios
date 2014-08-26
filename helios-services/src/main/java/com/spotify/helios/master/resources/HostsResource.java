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

import com.google.common.base.Optional;

import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.HostDeregisterResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.SetGoalResponse;
import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.master.HostStillInUseException;
import com.spotify.helios.master.JobAlreadyDeployedException;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.JobNotDeployedException;
import com.spotify.helios.master.JobPortAllocationConflictException;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.master.http.PATCH;
import com.yammer.metrics.annotation.ExceptionMetered;
import com.yammer.metrics.annotation.Timed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.validation.Valid;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.HOST_NOT_FOUND;
import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.INVALID_ID;
import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.JOB_NOT_FOUND;
import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.OK;
import static com.spotify.helios.master.http.Responses.badRequest;
import static com.spotify.helios.master.http.Responses.notFound;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/hosts")
public class HostsResource {

  private static final Logger log = LoggerFactory.getLogger(HostsResource.class);

  private final MasterModel model;

  public HostsResource(final MasterModel model) {
    this.model = model;
  }

  /**
   * Returns the list of hostnames of known hosts/agents.
   */
  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public List<String> list() {
    return model.listHosts();
  }

  /**
   * Registers a host with the cluster.  The {@code host} is the name of the host.  It SHOULD be
   * the hostname of the machine.  The {@code id} should be a persistent value for the host, but
   * initially randomly generated.  This way we don't have two machines claiming to be the same
   * host: at least by accident.
   */
  @PUT
  @Path("{host}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response.Status put(@PathParam("host") final String host,
                             @QueryParam("id") final String id) {
    model.registerHost(host, id);
    log.info("added host {}", host);
    return Response.Status.OK;
  }

  /**
   * Deregisters the host from the cluster.  Will delete just about everything the cluster knows
   * about it.
   */
  @DELETE
  @Path("{id}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public HostDeregisterResponse delete(@PathParam("id") final String host) {
    try {
      model.deregisterHost(host);
      return new HostDeregisterResponse(HostDeregisterResponse.Status.OK, host);
    } catch (HostNotFoundException e) {
      throw notFound(new HostDeregisterResponse(HostDeregisterResponse.Status.NOT_FOUND, host));
    } catch (HostStillInUseException e) {
      throw badRequest(new HostDeregisterResponse(HostDeregisterResponse.Status.JOBS_STILL_DEPLOYED,
                                                  host));
    }
  }

  /**
   * Returns various status information about the host.
   */
  @GET
  @Path("{id}/status")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Optional<HostStatus> hostStatus(@PathParam("id") final String host) {
    return Optional.fromNullable(model.getHostStatus(host));
  }

  /**
   * Sets the deployment of the job identified by its {@link JobId} on the host named by
   * {@code host} to {@code deployment}
   * @return
   */
  @PUT
  @Path("/{host}/jobs/{job}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public JobDeployResponse jobPut(@PathParam("host") final String host,
                                  @PathParam("job") final JobId jobId,
                                  @Valid final Deployment deployment) {
    if (!jobId.isFullyQualified()) {
      throw badRequest(new JobDeployResponse(JobDeployResponse.Status.INVALID_ID, host,
                                             jobId));
    }
    try {
      model.deployJob(host, deployment);
      return new JobDeployResponse(JobDeployResponse.Status.OK, host, jobId);
    } catch (JobAlreadyDeployedException e) {
      throw badRequest(new JobDeployResponse(JobDeployResponse.Status.JOB_ALREADY_DEPLOYED, host,
                                             jobId));
    } catch (HostNotFoundException e) {
      throw badRequest(new JobDeployResponse(JobDeployResponse.Status.HOST_NOT_FOUND, host, jobId));
    } catch (JobDoesNotExistException e) {
      throw badRequest(new JobDeployResponse(JobDeployResponse.Status.JOB_NOT_FOUND, host, jobId));
    } catch (JobPortAllocationConflictException e) {
      throw badRequest(new JobDeployResponse(JobDeployResponse.Status.PORT_CONFLICT, host, jobId));
    }
  }

  /**
   * Causes the job identified by its {@link JobId} to be undeployed from the specified host.
   * This call will fail if the host is not found or the job is not deployed on the host.
   */
  @DELETE
  @Path("/{host}/jobs/{job}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public JobUndeployResponse jobDelete(@PathParam("host") final String host,
                                       @PathParam("job") final JobId jobId) {
    if (!jobId.isFullyQualified()) {
      throw badRequest(new JobUndeployResponse(INVALID_ID, host, jobId));
    }
    try {
      model.undeployJob(host, jobId);
      return new JobUndeployResponse(OK, host, jobId);
    } catch (HostNotFoundException e) {
      throw notFound(new JobUndeployResponse(HOST_NOT_FOUND, host, jobId));
    } catch (JobNotDeployedException e) {
      throw notFound(new JobUndeployResponse(JOB_NOT_FOUND, host, jobId));
    }
  }

  /**
   * Alters the current deployment of a deployed job identified by it's job id on the specified
   * host.
   */
  @PATCH
  @Path("/{host}/jobs/{job}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public SetGoalResponse jobPatch(@PathParam("host") final String host,
                                  @PathParam("job") final JobId jobId,
                                  @Valid final Deployment deployment) {
    if (!deployment.getJobId().equals(jobId)) {
      throw badRequest(new SetGoalResponse(SetGoalResponse.Status.ID_MISMATCH, host, jobId));
    }
    try {
      model.updateDeployment(host, deployment);
    } catch (HostNotFoundException e) {
      throw notFound(new SetGoalResponse(SetGoalResponse.Status.HOST_NOT_FOUND, host, jobId));
    } catch (JobNotDeployedException e) {
      throw notFound(new SetGoalResponse(SetGoalResponse.Status.JOB_NOT_DEPLOYED, host, jobId));
    }
    log.info("patched job {} on host {}", deployment, host);
    return new SetGoalResponse(SetGoalResponse.Status.OK, host, jobId);
  }

  /**
   * Returns the current {@link Deployment} of {@code job} on {@code host} if it is deployed.
   */
  @GET
  @Path("/{host}/jobs/{job}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Optional<Deployment> jobGet(@PathParam("host") final String host,
                                     @PathParam("job") final JobId jobId) {
    if (!jobId.isFullyQualified()) {
      throw badRequest();
    }
    return Optional.fromNullable(model.getDeployment(host, jobId));
  }
}

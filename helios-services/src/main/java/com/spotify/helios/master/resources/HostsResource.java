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

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.common.descriptors.Job.EMPTY_TOKEN;
import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.FORBIDDEN;
import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.HOST_NOT_FOUND;
import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.INVALID_ID;
import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.JOB_NOT_FOUND;
import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.OK;
import static com.spotify.helios.master.http.Responses.badRequest;
import static com.spotify.helios.master.http.Responses.forbidden;
import static com.spotify.helios.master.http.Responses.notFound;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.HostDeregisterResponse;
import com.spotify.helios.common.protocol.HostRegisterResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.SetGoalResponse;
import com.spotify.helios.master.HostMatcher;
import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.master.HostStillInUseException;
import com.spotify.helios.master.JobAlreadyDeployedException;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.JobNotDeployedException;
import com.spotify.helios.master.JobPortAllocationConflictException;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.master.TokenVerificationException;
import com.spotify.helios.master.http.PATCH;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/hosts")
public class HostsResource {
  private static final Logger log = LoggerFactory.getLogger(HostsResource.class);

  private final MasterModel model;

  public HostsResource(final MasterModel model) {
    this.model = model;
  }

  /**
   * Returns the list of hostnames of known hosts/agents.
   *
   * @return The list of hostnames.
   */
  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public List<String> list(@QueryParam("namePattern") final String namePattern,
                           @QueryParam("selector") final List<String> hostSelectors) {

    List<String> hosts = namePattern == null ? model.listHosts() : model.listHosts(namePattern);

    if (!hostSelectors.isEmpty()) {
      // check that all supplied selectors are parseable/valid
      final List<HostSelector> selectors = hostSelectors.stream()
          .map(selectorStr -> {
            final HostSelector parsed = HostSelector.parse(selectorStr);
            if (parsed == null) {
              throw new WebApplicationException(
                  Response.status(Response.Status.BAD_REQUEST)
                      .entity("Invalid host selector: " + selectorStr)
                      .build()
              );
            }
            return parsed;
          })
          .collect(Collectors.toList());

      final Map<String, Map<String, String>> hostsAndLabels = getLabels(hosts);

      final HostMatcher matcher = new HostMatcher(hostsAndLabels);
      hosts = matcher.getMatchingHosts(selectors);
    }

    return hosts;
  }

  /**
   * Registers a host with the cluster.  The {@code host} is the name of the host.  It SHOULD be
   * the hostname of the machine.  The {@code id} should be a persistent value for the host, but
   * initially randomly generated.  This way we don't have two machines claiming to be the same
   * host: at least by accident.
   *
   * @param host The host to register.
   * @param id   The randomly generated ID for the host.
   *
   * @return The response.
   */
  @PUT
  @Path("{host}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response.Status put(@PathParam("host") final String host,
                             @QueryParam("id") @DefaultValue("") final String id) {
    if (isNullOrEmpty(id)) {
      throw badRequest(new HostRegisterResponse(HostRegisterResponse.Status.INVALID_ID, host));
    }

    model.registerHost(host, id);
    log.info("added host {}", host);
    return Response.Status.OK;
  }

  /**
   * Deregisters the host from the cluster.  Will delete just about everything the cluster knows
   * about it.
   *
   * @param host The host to deregister.
   *
   * @return The response.
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
   *
   * @param host         The host id.
   * @param statusFilter An optional status filter.
   *
   * @return The host status.
   */
  @GET
  @Path("{id}/status")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Optional<HostStatus> hostStatus(
      @PathParam("id") final String host,
      @QueryParam("status") @DefaultValue("") final String statusFilter) {
    final HostStatus status = model.getHostStatus(host);
    final Optional<HostStatus> response;
    if (status != null
        && (isNullOrEmpty(statusFilter) || statusFilter.equals(status.getStatus().toString()))) {
      response = Optional.of(status);
    } else {
      response = Optional.absent();
    }
    log.debug("hostStatus: host={}, statusFilter={}, returning: {}", host, statusFilter, response);
    return response;
  }

  /**
   * Returns various status information about the hosts.
   *
   * @param hosts        The hosts.
   * @param statusFilter An optional status filter.
   *
   * @return The response.
   */
  @POST
  @Path("/statuses")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Map<String, HostStatus> hostStatuses(
      final List<String> hosts,
      @QueryParam("status") @DefaultValue("") final String statusFilter) {
    final Map<String, HostStatus> statuses = Maps.newHashMap();
    for (final String host : hosts) {
      final HostStatus status = model.getHostStatus(host);
      if (status != null) {
        if (isNullOrEmpty(statusFilter) || statusFilter.equals(status.getStatus().toString())) {
          statuses.put(host, status);
        }
      }
    }
    return statuses;
  }

  /**
   * Sets the deployment of the job identified by its {@link JobId} on the host named by
   * {@code host} to {@code deployment}.
   *
   * @param host       The host to deploy to.
   * @param jobId      The job to deploy.
   * @param deployment Deployment information.
   * @param username   The user deploying.
   * @param token      The authorization token for this deployment.
   *
   * @return The response.
   */

  @PUT
  @Path("/{host}/jobs/{job}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public JobDeployResponse jobPut(
      @PathParam("host") final String host,
      @PathParam("job") final JobId jobId,
      @Valid final Deployment deployment,
      @RequestUser final String username,
      @QueryParam("token") @DefaultValue(EMPTY_TOKEN) final String token) {
    if (!jobId.isFullyQualified()) {
      throw badRequest(new JobDeployResponse(JobDeployResponse.Status.INVALID_ID, host, jobId));
    }
    try {
      final Deployment actualDeployment = deployment.toBuilder().setDeployerUser(username).build();
      model.deployJob(host, actualDeployment, token);
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
    } catch (TokenVerificationException e) {
      throw forbidden(new JobDeployResponse(JobDeployResponse.Status.FORBIDDEN, host, jobId));
    }
  }

  /**
   * Causes the job identified by its {@link JobId} to be undeployed from the specified host.
   * This call will fail if the host is not found or the job is not deployed on the host.
   *
   * @param host  The host to undeploy from.
   * @param jobId The job to undeploy.
   * @param token The authorization token.
   *
   * @return The response.
   */
  @DELETE
  @Path("/{host}/jobs/{job}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public JobUndeployResponse jobDelete(@PathParam("host") final String host,
                                       @PathParam("job") final JobId jobId,
                                       @QueryParam("token") @DefaultValue("") final String token) {
    if (!jobId.isFullyQualified()) {
      throw badRequest(new JobUndeployResponse(INVALID_ID, host, jobId));
    }
    try {
      model.undeployJob(host, jobId, token);
      return new JobUndeployResponse(OK, host, jobId);
    } catch (HostNotFoundException e) {
      throw notFound(new JobUndeployResponse(HOST_NOT_FOUND, host, jobId));
    } catch (JobNotDeployedException e) {
      throw notFound(new JobUndeployResponse(JOB_NOT_FOUND, host, jobId));
    } catch (TokenVerificationException e) {
      throw forbidden(new JobUndeployResponse(FORBIDDEN, host, jobId));
    }
  }

  /**
   * Alters the current deployment of a deployed job identified by its job id on the specified
   * host.
   *
   * @param host       The host.
   * @param jobId      The ID of the job.
   * @param deployment The new deployment.
   * @param token      The authorization token for this job.
   *
   * @return The response.
   */
  @PATCH
  @Path("/{host}/jobs/{job}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public SetGoalResponse jobPatch(@PathParam("host") final String host,
                                  @PathParam("job") final JobId jobId,
                                  @Valid final Deployment deployment,
                                  @QueryParam("token") @DefaultValue("") final String token) {
    if (!deployment.getJobId().equals(jobId)) {
      throw badRequest(new SetGoalResponse(SetGoalResponse.Status.ID_MISMATCH, host, jobId));
    }
    try {
      model.updateDeployment(host, deployment, token);
    } catch (HostNotFoundException e) {
      throw notFound(new SetGoalResponse(SetGoalResponse.Status.HOST_NOT_FOUND, host, jobId));
    } catch (JobNotDeployedException e) {
      throw notFound(new SetGoalResponse(SetGoalResponse.Status.JOB_NOT_DEPLOYED, host, jobId));
    } catch (TokenVerificationException e) {
      throw forbidden(new SetGoalResponse(SetGoalResponse.Status.FORBIDDEN, host, jobId));
    }
    log.info("patched job {} on host {}", deployment, host);
    return new SetGoalResponse(SetGoalResponse.Status.OK, host, jobId);
  }

  /**
   * Returns the current {@link Deployment} of {@code job} on {@code host} if it is deployed.
   *
   * @param host  The host where the job is deployed.
   * @param jobId The ID of the job.
   *
   * @return The response.
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

  private Map<String, Map<String, String>> getLabels(final List<String> hosts) {
    return hosts.stream().collect(Collectors.toMap(Function.identity(), model::getHostLabels));
  }
}

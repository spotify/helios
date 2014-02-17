package com.spotify.helios.master.resources;

import com.google.common.base.Optional;

import com.spotify.helios.common.AgentDoesNotExistException;
import com.spotify.helios.common.JobAlreadyDeployedException;
import com.spotify.helios.common.JobDoesNotExistException;
import com.spotify.helios.common.JobNotDeployedException;
import com.spotify.helios.common.JobPortAllocationConflictException;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.AgentDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.SetGoalResponse;
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
import javax.ws.rs.core.Response;

import static com.spotify.helios.common.protocol.JobUndeployResponse.Status.AGENT_NOT_FOUND;
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

  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public List<String> list() {
    return model.getAgents();
  }

  @PUT
  @Path("{id}")
  @Timed
  @ExceptionMetered
  public Response.Status put(@PathParam("id") final String agent) {
    model.addAgent(agent);
    log.info("added host {}", agent);
    return Response.Status.OK;
  }


  @DELETE
  @Path("{id}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public AgentDeleteResponse delete(@PathParam("id") final String agent) {
    try {
      model.removeAgent(agent);
      return new AgentDeleteResponse(AgentDeleteResponse.Status.OK, agent);
    } catch (AgentDoesNotExistException e) {
      throw notFound(new AgentDeleteResponse(AgentDeleteResponse.Status.NOT_FOUND, agent));
    }
  }

  @GET
  @Path("{id}/status")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Optional<AgentStatus> hostStatus(@PathParam("id") final String agent) {
    return Optional.fromNullable(model.getAgentStatus(agent));
  }

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
    } catch (AgentDoesNotExistException e) {
      throw badRequest(new JobDeployResponse(JobDeployResponse.Status.AGENT_NOT_FOUND, host,
                                             jobId));
    } catch (JobDoesNotExistException e) {
      throw badRequest(new JobDeployResponse(JobDeployResponse.Status.JOB_NOT_FOUND, host, jobId));
    } catch (JobPortAllocationConflictException e) {
      throw badRequest(new JobDeployResponse(JobDeployResponse.Status.PORT_CONFLICT, host, jobId));
    }
  }

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
    } catch (AgentDoesNotExistException e) {
      throw notFound(new JobUndeployResponse(AGENT_NOT_FOUND, host, jobId));
    } catch (JobNotDeployedException e) {
      throw notFound(new JobUndeployResponse(JOB_NOT_FOUND, host, jobId));
    }
  }

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
    } catch (AgentDoesNotExistException e) {
      throw notFound(new SetGoalResponse(SetGoalResponse.Status.AGENT_NOT_FOUND, host, jobId));
    } catch (JobNotDeployedException e) {
      throw notFound(new SetGoalResponse(SetGoalResponse.Status.JOB_NOT_DEPLOYED, host, jobId));
    }
    log.info("deployed job {} to host {}", deployment, host);
    return new SetGoalResponse(SetGoalResponse.Status.OK, host, jobId);
  }

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

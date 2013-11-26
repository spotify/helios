/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.google.protobuf.ByteString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.helios.common.AgentDoesNotExistException;
import com.spotify.helios.common.JobNotDeployedException;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.JobAlreadyDeployedException;
import com.spotify.helios.common.JobDoesNotExistException;
import com.spotify.helios.common.JobExistsException;
import com.spotify.helios.common.JobStillInUseException;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobIdParseException;
import com.spotify.helios.common.protocol.AgentDeleteResponse;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse.Status;
import com.spotify.helios.common.protocol.SetGoalResponse;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.message.StatusCode;
import com.spotify.hermes.service.RequestHandlerException;
import com.spotify.hermes.service.ServiceRequest;
import com.spotify.hermes.service.handlers.MatchingHandler;
import com.spotify.hermes.util.Match;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static com.spotify.hermes.message.StatusCode.BAD_REQUEST;
import static com.spotify.hermes.message.StatusCode.FORBIDDEN;
import static com.spotify.hermes.message.StatusCode.NOT_FOUND;
import static com.spotify.hermes.message.StatusCode.OK;
import static com.spotify.hermes.message.StatusCode.SERVER_ERROR;

public class MasterHandler extends MatchingHandler {

  private final Logger log = LoggerFactory.getLogger(MasterHandler.class);
  private final MasterModel model;

  public MasterHandler(final MasterModel model) {
    this.model = model;
  }

  @Match(uri = "hm://helios/jobs/<id>", methods = "PUT")
  public void jobPut(final ServiceRequest request, final String id) {
    final Message message = request.getMessage();
    if (message.getPayloads().size() != 1) {
      throw new RequestHandlerException(BAD_REQUEST);
    }

    final byte[] payload = message.getPayloads().get(0).toByteArray();
    final Job descriptor;
    try {
      descriptor = parse(payload, Job.class);
    } catch (IOException e) {
      throw new RequestHandlerException(BAD_REQUEST);
    }

    if (!descriptor.getId().equals(parseJobId(id))) {
      respond(request, BAD_REQUEST, new CreateJobResponse(CreateJobResponse.Status.ID_MISMATCH));
      return;
    }

    try {
      model.addJob(descriptor);
    } catch (JobExistsException e) {
      respond(request, BAD_REQUEST,
              new CreateJobResponse(CreateJobResponse.Status.JOB_ALREADY_EXISTS));
      return;
    } catch (HeliosException e) {
      log.error("failed to add job: {}:{}", id, descriptor, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    log.info("added job {}:{}", id, descriptor);

    respond(request, OK, new CreateJobResponse(CreateJobResponse.Status.OK));
  }

  @Match(uri = "hm://helios/jobs/<id>", methods = "GET")
  public void jobGet(final ServiceRequest request, final String id) {
    final JobId jobId = parseJobId(id);
    try {
      final Job job = model.getJob(jobId);
      ok(request, job);
    } catch (HeliosException e) {
      log.error("failed to get job: {}", id, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/jobs/", methods = "GET")
  public void jobsGet(final ServiceRequest request) {
    try {
      final Map<JobId, Job> jobs = model.getJobs();
      ok(request, jobs);
    } catch (HeliosException e) {
      log.error("failed to get jobs", e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/jobs/<id>", methods = "DELETE")
  public void jobDelete(final ServiceRequest request, final String id) {
    try {
      model.removeJob(parseJobId(id));
      respond(request, OK, new JobDeleteResponse(JobDeleteResponse.Status.OK));
    } catch (JobStillInUseException e) {
      respond(request, FORBIDDEN, new JobDeleteResponse(JobDeleteResponse.Status.STILL_IN_USE));
    } catch (HeliosException e) {
      log.error("failed to remove job: {}", id, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/jobs/<id>/status", methods = "GET")
  public void jobStatusGet(final ServiceRequest request, final String id) {
    final JobId jobId = parseJobId(id);
    try {
      final JobStatus jobStatus = model.getJobStatus(jobId);
      ok(request, jobStatus);
    } catch (HeliosException e) {
      log.error("failed to get job status for job: {}", id, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/agents/<agent>", methods = "PUT")
  public void agentPut(final ServiceRequest request, final String agent) {
    try {
      model.addAgent(agent);
    } catch (HeliosException e) {
      log.error("failed to add agent {}", agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    log.info("added agent {}", agent);

    ok(request);
  }

  @Match(uri = "hm://helios/agents/<agent>/jobs/<job>", methods = "PUT")
  public void agentJobPut(final ServiceRequest request, final String agent,
                          final String job)
      throws RequestHandlerException {
    final Deployment deployment = parseDeployment(request);

    final JobId jobId;
    try {
      jobId = JobId.parse(job);
    } catch (JobIdParseException e) {
      respond(request, BAD_REQUEST,
              new JobDeployResponse(JobDeployResponse.Status.INVALID_ID, agent, job));
      return;
    }

    if (!deployment.getJobId().equals(jobId)) {
      respond(request, BAD_REQUEST,
              new JobDeployResponse(JobDeployResponse.Status.ID_MISMATCH, agent, job));
      return;
    }

    StatusCode code = OK;
    JobDeployResponse.Status detailStatus = JobDeployResponse.Status.OK;

    try {
      model.deployJob(agent, deployment);
    } catch (JobDoesNotExistException e) {
      code = NOT_FOUND;
      detailStatus = JobDeployResponse.Status.JOB_NOT_FOUND;
    } catch (AgentDoesNotExistException e) {
      code = NOT_FOUND;
      detailStatus = JobDeployResponse.Status.AGENT_NOT_FOUND;
    } catch (JobAlreadyDeployedException e) {
      code = StatusCode.METHOD_NOT_ALLOWED;
      detailStatus = JobDeployResponse.Status.JOB_ALREADY_DEPLOYED;
    } catch (HeliosException e) {
      log.error("failed to add job {} to agent {}", deployment, agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    log.info("added job {} to agent {}", deployment, agent);

    respond(request, code, new JobDeployResponse(detailStatus, agent, job));
  }

  @Match(uri = "hm://helios/agents/<agent>/jobs/<id>", methods = "PATCH")
  public void jobPatch(final ServiceRequest request,
                       final String agent,
                       final String job) {
    final Deployment deployment = parseDeployment(request);

    final JobId jobId;
    try {
      jobId = JobId.parse(job);
    } catch (JobIdParseException e) {
      respond(request, BAD_REQUEST,
              new SetGoalResponse(SetGoalResponse.Status.INVALID_ID, agent, job));
      return;
    }

    if (!deployment.getJobId().equals(jobId)) {
      respond(request, BAD_REQUEST,
              new SetGoalResponse(SetGoalResponse.Status.ID_MISMATCH, agent, job));
      return;
    }

    StatusCode code = OK;
    SetGoalResponse.Status detailStatus = SetGoalResponse.Status.OK;

    try {
      model.updateDeployment(agent, deployment);
    } catch (JobDoesNotExistException e) {
      code = NOT_FOUND;
      detailStatus = SetGoalResponse.Status.JOB_NOT_FOUND;
    } catch (AgentDoesNotExistException e) {
      code = NOT_FOUND;
      detailStatus = SetGoalResponse.Status.AGENT_NOT_FOUND;
    } catch (JobNotDeployedException e) {
      code = NOT_FOUND;
      detailStatus = SetGoalResponse.Status.JOB_NOT_DEPLOYED;
    } catch (HeliosException e) {
      log.error("failed to add job {} to agent {}", deployment, agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    log.info("added job {} to agent {}", deployment, agent);

    respond(request, code, new SetGoalResponse(detailStatus, agent, job));
  }

  private Deployment parseDeployment(final ServiceRequest request) {
    final Message message = request.getMessage();
    if (message.getPayloads().size() != 1) {
      throw new RequestHandlerException(BAD_REQUEST);
    }

    final byte[] payload = message.getPayloads().get(0).toByteArray();
    final Deployment deployment;
    try {
      deployment = Json.read(payload, Deployment.class);
    } catch (IOException e) {
      throw new RequestHandlerException(BAD_REQUEST);
    }
    return deployment;
  }

  @Match(uri = "hm://helios/agents/<agent>/jobs/<job>", methods = "GET")
  public void agentJobGet(final ServiceRequest request, final String agent,
                          final String jobId)
      throws RequestHandlerException {

    final Deployment deployment;
    try {
      deployment = model.getDeployment(agent, parseJobId(jobId));
    } catch (HeliosException e) {
      log.error("failed to get job {} for agent {}", jobId, agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    if (deployment == null) {
      request.reply(NOT_FOUND);
      return;
    }

    ok(request, deployment);
  }

  @Match(uri = "hm://helios/agents/<agent>", methods = "DELETE")
  public void agentDelete(final ServiceRequest request, final String agent) {
    try {
      model.removeAgent(agent);
    } catch (AgentDoesNotExistException e) {
      respond(request, NOT_FOUND,
              new AgentDeleteResponse(AgentDeleteResponse.Status.NOT_FOUND, agent));
      return;
    } catch (HeliosException e) {
      log.error("failed to remove agent {}", agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
    respond(request, NOT_FOUND,
            new AgentDeleteResponse(AgentDeleteResponse.Status.OK, agent));
  }

  @Match(uri = "hm://helios/agents/<agent>/jobs/<job>", methods = "DELETE")
  public void agentJobDelete(final ServiceRequest request, final String agent,
                             final String jobId)
      throws RequestHandlerException {

    StatusCode code = OK;
    Status detail = JobUndeployResponse.Status.OK;
    try {
      model.undeployJob(agent, parseJobId(jobId));
    } catch (AgentDoesNotExistException e) {
      code = NOT_FOUND;
      detail = JobUndeployResponse.Status.AGENT_NOT_FOUND;
    } catch (JobDoesNotExistException e) {
      code = NOT_FOUND;
      detail = JobUndeployResponse.Status.JOB_NOT_FOUND;
    } catch (HeliosException e) {
      log.error("failed to remove job {} from agent {}", jobId, agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    respond(request, code, new JobUndeployResponse(detail, agent, jobId));
  }

  @Match(uri = "hm://helios/agents/<agent>/status", methods = "GET")
  public void agentStatusGet(final ServiceRequest request, final String agent)
      throws RequestHandlerException {

    final AgentStatus agentStatus;
    try {
      agentStatus = model.getAgentStatus(agent);
    } catch (HeliosException e) {
      log.error("failed to get status for agent {}", agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
    if (agent == null) {
      request.reply(NOT_FOUND);
      return;
    }

    ok(request, agentStatus);
  }

  @Match(uri = "hm://helios/agents/", methods = "GET")
  public void agentsGet(final ServiceRequest request)
      throws RequestHandlerException {
    try {
      ok(request, model.getAgents());
    } catch (HeliosException e) {
      log.error("getting agents failed", e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/masters/", methods = "GET")
  public void mastersGet(final ServiceRequest request)
      throws RequestHandlerException, JsonProcessingException {
    // TODO(drewc): should make it so we can get all masters, not just the running ones
    try {
      ok(request, model.getRunningMasters());
    } catch (HeliosException e) {
      log.error("getting masters failed", e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  private void ok(final ServiceRequest request) {
    request.reply(OK);
  }

  private void ok(final ServiceRequest request, final Object payload) {
    respond(request, StatusCode.OK, payload);
  }

  private void respond(final ServiceRequest request, StatusCode code, final Object payload) {
    final byte[] json;
    json = Json.asBytesUnchecked(payload);
    final Message reply = request.getMessage()
        .makeReplyBuilder(code)
        .appendPayload(ByteString.copyFrom(json))
        .build();
    request.reply(reply);
  }

  private JobId parseJobId(final String id) {
    final JobId jobId;
    try {
      jobId = JobId.parse(id);
    } catch (JobIdParseException e) {
      throw new RequestHandlerException(BAD_REQUEST);
    }
    return jobId;
  }
}

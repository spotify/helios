/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.helios.common.AgentDoesNotExistException;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.JobAlreadyDeployedException;
import com.spotify.helios.common.JobDoesNotExistException;
import com.spotify.helios.common.JobExistsException;
import com.spotify.helios.common.JobNotDeployedException;
import com.spotify.helios.common.JobPortAllocationConflictException;
import com.spotify.helios.common.JobStillInUseException;
import com.spotify.helios.common.JobValidator;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.Version;
import com.spotify.helios.common.VersionCheckResponse;
import com.spotify.helios.common.VersionCompatibility;
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
import com.spotify.helios.common.protocol.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;
import com.spotify.helios.common.statistics.MasterMetrics;
import com.spotify.helios.common.statistics.MetricsContext;
import com.spotify.helios.common.statistics.RequestType;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.message.StatusCode;
import com.spotify.hermes.service.RequestHandlerException;
import com.spotify.hermes.service.ServiceRequest;
import com.spotify.hermes.service.handlers.MatchingHandler;
import com.spotify.hermes.util.Match;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static com.spotify.hermes.message.StatusCode.BAD_REQUEST;
import static com.spotify.hermes.message.StatusCode.FORBIDDEN;
import static com.spotify.hermes.message.StatusCode.NOT_FOUND;
import static com.spotify.hermes.message.StatusCode.OK;
import static com.spotify.hermes.message.StatusCode.SERVER_ERROR;

public class MasterHandler extends MatchingHandler {

  private static final Logger log = LoggerFactory.getLogger(MasterHandler.class);

  private static final JobValidator JOB_VALIDATOR = new JobValidator();

  private final MasterModel model;

  private final MasterMetrics metrics;

  public MasterHandler(final MasterModel model, MasterMetrics metrics) {
    this.model = model;
    this.metrics = metrics;
  }

  public String safeURLDecode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("URL Decoding failed for " + s, e);
    }
  }

  @Match(uri = "hm://helios/version_check/<version>", methods = "GET")
  public void versionCheck(final ServiceRequest request, final String rawVersion) {
    final PomVersion clientVersion = PomVersion.parse(safeURLDecode(rawVersion));
    final PomVersion serverVersion = PomVersion.parse(Version.POM_VERSION);

    final VersionCompatibility.Status status = VersionCompatibility.getStatus(serverVersion,
        clientVersion);
    final VersionCheckResponse resp = new VersionCheckResponse(
        status,
        serverVersion,
        Version.RECOMMENDED_VERSION);
    respond(request, OK, resp);
  }

  @Match(uri = "hm://helios/jobs/<id>", methods = "PUT")
  public void jobPut(final ServiceRequest request, final String rawId) {
    MetricsContext context = metrics.beginRequest(RequestType.JOB_CREATE);
    final Message message = request.getMessage();
    if (message.getPayloads().size() != 1) {
      context.userError();
      throw new RequestHandlerException(BAD_REQUEST);
    }

    final byte[] payload = message.getPayloads().get(0).toByteArray();
    final Job job;
    try {
      job = parse(payload, Job.class);
    } catch (IOException e) {
      context.userError();
      throw new RequestHandlerException(BAD_REQUEST);
    }

    final String id = safeURLDecode(rawId);
    if (!job.getId().equals(parseJobId(id))) {
      context.userError();
      respond(request, BAD_REQUEST, new CreateJobResponse(CreateJobResponse.Status.ID_MISMATCH));
      return;
    }

    final Collection<String> errors = JOB_VALIDATOR.validate(job);
    if (!errors.isEmpty()) {
      context.userError();
      respond(request, BAD_REQUEST, new CreateJobResponse(
          CreateJobResponse.Status.INVALID_JOB_DEFINITION));
      return;
    }

    try {
      model.addJob(job);
      context.success();
    } catch (JobExistsException e) {
      context.userError();
      respond(request, BAD_REQUEST,
              new CreateJobResponse(CreateJobResponse.Status.JOB_ALREADY_EXISTS));
      return;
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to add job: {}:{}", id, job, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    log.info("added job {}:{}", id, job);

    respond(request, OK, new CreateJobResponse(CreateJobResponse.Status.OK));
  }

  @Match(uri = "hm://helios/jobs/<id>", methods = "GET")
  public void jobGet(final ServiceRequest request, final String rawId) {
    MetricsContext context = metrics.beginRequest(RequestType.JOB_GET);
    final String id = safeURLDecode(rawId);
    final JobId jobId = parseJobId(id);
    try {
      final Job job = model.getJob(jobId);
      context.success();
      ok(request, job);
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to get job: {}", id, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/jobs", methods = "GET")
  public void jobsGet(final ServiceRequest request) {
    MetricsContext context = metrics.beginRequest(RequestType.JOB_LIST);
    final String q = request.getMessage().getParameter("q");
    try {
      final Map<JobId, Job> allJobs = model.getJobs();
      if (isNullOrEmpty(q)) {
        // Return all jobs
        context.success();
        metrics.jobsInJobList(allJobs.size());
        ok(request, allJobs);
      } else {
        // Filter jobs
        // TODO (dano): support prefix matching queries?
        final JobId needle = JobId.parse(q);
        final Map<JobId, Job> filteredJobs = Maps.newHashMap();
        for (final JobId jobId : allJobs.keySet()) {
          if (needle.getName().equals(jobId.getName()) &&
              (needle.getVersion() == null || needle.getVersion().equals(jobId.getVersion())) &&
              (needle.getHash() == null || needle.getHash().equals((jobId.getHash())))) {
            filteredJobs.put(jobId, allJobs.get(jobId));
          }
        }
        context.success();
        metrics.jobsInJobList(filteredJobs.size());
        ok(request, filteredJobs);
      }
    } catch (JobIdParseException e) {
      context.userError();
      log.error("failed to parse job id query, e");
      throw new RequestHandlerException(BAD_REQUEST);
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to get jobs", e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/jobs/<id>", methods = "DELETE")
  public void jobDelete(final ServiceRequest request, final String rawId) {
    MetricsContext context = metrics.beginRequest(RequestType.JOB_DELETE);
    final String id = safeURLDecode(rawId);
    try {
      model.removeJob(parseJobId(id));
      context.success();
      respond(request, OK, new JobDeleteResponse(JobDeleteResponse.Status.OK));
    } catch (JobStillInUseException e) {
      context.userError();
      respond(request, FORBIDDEN, new JobDeleteResponse(JobDeleteResponse.Status.STILL_IN_USE));
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to remove job: {}", id, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/jobs/<id>/status", methods = "GET")
  public void jobStatusGet(final ServiceRequest request, final String rawId) {
    MetricsContext context = metrics.beginRequest(RequestType.JOB_STATUS);
    final String id = safeURLDecode(rawId);
    final JobId jobId = parseJobId(id);
    try {
      final JobStatus jobStatus = model.getJobStatus(jobId);
      context.success();
      ok(request, jobStatus);
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to get job status for job: {}", id, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/agents/<agent>", methods = "PUT")
  public void agentPut(final ServiceRequest request, final String rawAgent) {
    MetricsContext context = metrics.beginRequest(RequestType.AGENT_CREATE);
    final String agent = safeURLDecode(rawAgent);
    try {
      model.addAgent(agent);
      context.success();
      log.info("added agent {}", agent);

      ok(request);
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to add agent {}", agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/agents/<agent>/jobs/<job>", methods = "PUT")
  public void agentJobPut(final ServiceRequest request, final String rawAgent,
                          final String rawJob)
      throws RequestHandlerException {
    MetricsContext context = metrics.beginRequest(RequestType.JOB_DEPLOY);
    final String agent = safeURLDecode(rawAgent);
    final String job = safeURLDecode(rawJob);
    final Deployment deployment = parseDeployment(request);

    final JobId jobId;
    try {
      jobId = JobId.parse(job);
    } catch (JobIdParseException e) {
      context.userError();
      respond(request, BAD_REQUEST,
              new JobDeployResponse(JobDeployResponse.Status.INVALID_ID, agent, job));
      return;
    }

    if (!deployment.getJobId().equals(jobId)) {
      context.userError();
      respond(request, BAD_REQUEST,
              new JobDeployResponse(JobDeployResponse.Status.ID_MISMATCH, agent, job));
      return;
    }

    StatusCode code = OK;
    JobDeployResponse.Status detailStatus = JobDeployResponse.Status.OK;

    try {
      model.deployJob(agent, deployment);
      log.info("added job {} to agent {}", deployment, agent);
      context.success();
    } catch (JobPortAllocationConflictException e){
      context.userError();
      code = BAD_REQUEST;
      detailStatus = JobDeployResponse.Status.PORT_CONFLICT;
    } catch (JobDoesNotExistException e) {
      context.userError();
      code = NOT_FOUND;
      detailStatus = JobDeployResponse.Status.JOB_NOT_FOUND;
    } catch (AgentDoesNotExistException e) {
      context.userError();
      code = NOT_FOUND;
      detailStatus = JobDeployResponse.Status.AGENT_NOT_FOUND;
    } catch (JobAlreadyDeployedException e) {
      context.userError();
      code = StatusCode.METHOD_NOT_ALLOWED;
      detailStatus = JobDeployResponse.Status.JOB_ALREADY_DEPLOYED;
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to add job {} to agent {}", deployment, agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    respond(request, code, new JobDeployResponse(detailStatus, agent, job));
  }

  @Match(uri = "hm://helios/agents/<agent>/jobs/<id>", methods = "PATCH")
  public void jobPatch(final ServiceRequest request,
                       final String rawAgent,
                       final String rawJob) {
    MetricsContext context = metrics.beginRequest(RequestType.JOB_PATCH);
    final Deployment deployment = parseDeployment(request);
    final String job = safeURLDecode(rawJob);
    final String agent = safeURLDecode(rawAgent);
    final JobId jobId;
    try {
      jobId = JobId.parse(job);
    } catch (JobIdParseException e) {
      context.userError();
      respond(request, BAD_REQUEST,
              new SetGoalResponse(SetGoalResponse.Status.INVALID_ID, agent, job));
      return;
    }

    if (!deployment.getJobId().equals(jobId)) {
      context.userError();
      respond(request, BAD_REQUEST,
              new SetGoalResponse(SetGoalResponse.Status.ID_MISMATCH, agent, job));
      return;
    }

    StatusCode code = OK;
    SetGoalResponse.Status detailStatus = SetGoalResponse.Status.OK;

    try {
      model.updateDeployment(agent, deployment);
      context.success();
    } catch (JobDoesNotExistException e) {
      context.userError();
      code = NOT_FOUND;
      detailStatus = SetGoalResponse.Status.JOB_NOT_FOUND;
    } catch (AgentDoesNotExistException e) {
      context.userError();
      code = NOT_FOUND;
      detailStatus = SetGoalResponse.Status.AGENT_NOT_FOUND;
    } catch (JobNotDeployedException e) {
      context.userError();
      code = NOT_FOUND;
      detailStatus = SetGoalResponse.Status.JOB_NOT_DEPLOYED;
    } catch (HeliosException e) {
      context.failure();
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
  public void agentJobGet(final ServiceRequest request, final String rawAgent,
                          final String rawJobId)
      throws RequestHandlerException {
    MetricsContext context = metrics.beginRequest(RequestType.AGENT_GET_DEPLOYMENT);
    final String agent = safeURLDecode(rawAgent);
    final String jobId = safeURLDecode(rawJobId);
    final Deployment deployment;
    try {
      deployment = model.getDeployment(agent, parseJobId(jobId));
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to get job {} for agent {}", jobId, agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    if (deployment == null) {
      context.userError();
      request.reply(NOT_FOUND);
      return;
    }

    context.success();
    ok(request, deployment);
  }

  @Match(uri = "hm://helios/agents/<agent>", methods = "DELETE")
  public void agentDelete(final ServiceRequest request, final String rawAgent) {
    MetricsContext context = metrics.beginRequest(RequestType.AGENT_DELETE);
    final String agent = safeURLDecode(rawAgent);
    try {
      model.removeAgent(agent);
      context.success();
      respond(request, OK,
          new AgentDeleteResponse(AgentDeleteResponse.Status.OK, agent));
    } catch (AgentDoesNotExistException e) {
      context.userError();
      respond(request, NOT_FOUND,
              new AgentDeleteResponse(AgentDeleteResponse.Status.NOT_FOUND, agent));
      return;
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to remove agent {}", agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/agents/<agent>/jobs/<job>", methods = "DELETE")
  public void agentJobDelete(final ServiceRequest request, final String rawAgent,
                             final String rawJobId)
      throws RequestHandlerException {
    MetricsContext context = metrics.beginRequest(RequestType.JOB_UNDEPLOY);
    final String agent = safeURLDecode(rawAgent);
    final String jobId = safeURLDecode(rawJobId);
    StatusCode code = OK;
    Status detail = JobUndeployResponse.Status.OK;
    try {
      model.undeployJob(agent, parseJobId(jobId));
      context.success();
    } catch (AgentDoesNotExistException e) {
      context.userError();
      code = NOT_FOUND;
      detail = JobUndeployResponse.Status.AGENT_NOT_FOUND;
    } catch (JobDoesNotExistException e) {
      context.userError();
      code = NOT_FOUND;
      detail = JobUndeployResponse.Status.JOB_NOT_FOUND;
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to remove job {} from agent {}", jobId, agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }

    respond(request, code, new JobUndeployResponse(detail, agent, jobId));
  }

  @Match(uri = "hm://helios/agents/<agent>/status", methods = "GET")
  public void agentStatusGet(final ServiceRequest request, final String rawAgent)
      throws RequestHandlerException {
    MetricsContext context = metrics.beginRequest(RequestType.AGENT_STATUS_GET);
    final String agent = safeURLDecode(rawAgent);
    final AgentStatus agentStatus;
    try {
      agentStatus = model.getAgentStatus(agent);
      context.success();
    } catch (HeliosException e) {
      context.failure();
      log.error("failed to get status for agent {}", agent, e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
    if (agent == null) {
      context.userError();
      request.reply(NOT_FOUND);
      return;
    }

    ok(request, agentStatus);
  }

  @Match(uri = "hm://helios/agents/", methods = "GET")
  public void agentsGet(final ServiceRequest request)
      throws RequestHandlerException {
    MetricsContext context = metrics.beginRequest(RequestType.AGENT_GET);
    try {
      ok(request, model.getAgents());
      context.success();
    } catch (HeliosException e) {
      context.failure();
      log.error("getting agents failed", e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/masters/", methods = "GET")
  public void mastersGet(final ServiceRequest request)
      throws RequestHandlerException, JsonProcessingException {
    MetricsContext context = metrics.beginRequest(RequestType.MASTER_LIST);
    // TODO(drewc): should make it so we can get all masters, not just the running ones
    try {
      ok(request, model.getRunningMasters());
      context.success();
    } catch (HeliosException e) {
      context.failure();
      log.error("getting masters failed", e);
      throw new RequestHandlerException(SERVER_ERROR);
    }
  }

  @Match(uri = "hm://helios/history/jobs/<jobid>", methods = "GET")
  public void jobHistoryGet(final ServiceRequest request, final String rawJobId)
      throws HeliosException, JobIdParseException, JsonProcessingException {
    MetricsContext context = metrics.beginRequest(RequestType.JOB_HISTORY);
    final String jobId = safeURLDecode(rawJobId);
    try {
      List<TaskStatusEvent> history = model.getJobHistory(JobId.parse(jobId));
      TaskStatusEvents events = new TaskStatusEvents(history, TaskStatusEvents.Status.OK);
      ok(request, events);
      metrics.jobsHistoryEventSize(events.getEvents().size());
      context.success();
    } catch (JobDoesNotExistException e) {
      context.failure();
      respond(request, NOT_FOUND, new TaskStatusEvents(ImmutableList.<TaskStatusEvent>of(),
          TaskStatusEvents.Status.JOB_ID_NOT_FOUND));
    }
  }

  private void ok(final ServiceRequest request) {
    request.reply(OK);
  }

  private void ok(final ServiceRequest request, final Object payload) {
    respond(request, StatusCode.OK, payload);
  }

  private void respond(final ServiceRequest request, StatusCode code, final Object payload) {
    final byte[] json = Json.asBytesUnchecked(payload);
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

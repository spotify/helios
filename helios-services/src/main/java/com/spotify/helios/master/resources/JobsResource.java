package com.spotify.helios.master.resources;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.JobDoesNotExistException;
import com.spotify.helios.common.JobExistsException;
import com.spotify.helios.common.JobStillDeployedException;
import com.spotify.helios.common.JobValidator;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobIdParseException;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobStatus;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import com.sun.jersey.api.core.InjectParam;
import com.yammer.metrics.annotation.ExceptionMetered;
import com.yammer.metrics.annotation.Timed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

import javax.validation.Valid;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import static com.spotify.helios.common.protocol.CreateJobResponse.Status.INVALID_JOB_DEFINITION;
import static com.spotify.helios.common.protocol.CreateJobResponse.Status.JOB_ALREADY_EXISTS;
import static com.spotify.helios.master.http.Responses.badRequest;
import static com.spotify.helios.master.http.Responses.notFound;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/jobs")
public class JobsResource {

  private static final Logger log = LoggerFactory.getLogger(JobsResource.class);

  private static final JobValidator JOB_VALIDATOR = new JobValidator();

  private final MasterModel model;
  private final MasterMetrics metrics;

  public JobsResource(final MasterModel model, final MasterMetrics metrics) {
    this.model = model;
    this.metrics = metrics;
  }

  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Map<JobId, Job> list(@QueryParam("q") @DefaultValue("") final String q)
      throws HeliosException {
    final Map<JobId, Job> allJobs = model.getJobs();

    // Return all jobs if the query string is empty
    if (q.isEmpty()) {
      metrics.jobsInJobList(allJobs.size());
      return allJobs;
    }

    // Filter jobs
    // TODO (dano): support prefix matching queries?
    final JobId needle;
    try {
      needle = JobId.parse(q);
    } catch (JobIdParseException e1) {
      log.error("failed to parse job id query, e");
      throw badRequest();
    }

    final Map<JobId, Job> filteredJobs = Maps.newHashMap();
    for (final JobId jobId : allJobs.keySet()) {
      if (needle.getName().equals(jobId.getName()) &&
          (needle.getVersion() == null || needle.getVersion().equals(jobId.getVersion())) &&
          (needle.getHash() == null || jobId.getHash().startsWith(needle.getHash()))) {
        filteredJobs.put(jobId, allJobs.get(jobId));
      }
    }

    metrics.jobsInJobList(filteredJobs.size());
    return filteredJobs;
  }


  @Path("{id}")
  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Optional<Job> get(@InjectParam @PathParam("id") @Valid final JobId id)
      throws HeliosException {
    if (!id.isFullyQualified()) {
      throw badRequest("Invalid id");
    }
    return Optional.fromNullable(model.getJob(id));
  }

  @POST
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public CreateJobResponse post(@Valid final Job job) throws HeliosException {
    final Collection<String> errors = JOB_VALIDATOR.validate(job);
    if (!errors.isEmpty()) {
      throw badRequest(new CreateJobResponse(INVALID_JOB_DEFINITION));
    }
    try {
      model.addJob(job);
    } catch (JobExistsException e) {
      throw badRequest(new CreateJobResponse(JOB_ALREADY_EXISTS));
    }
    log.info("created job: {}", job);
    return new CreateJobResponse(CreateJobResponse.Status.OK);
  }

  @Path("{id}")
  @DELETE
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public JobDeleteResponse delete(@PathParam("id") @Valid final JobId id) throws HeliosException {
    if (!id.isFullyQualified()) {
      throw badRequest("Invalid id");
    }
    try {
      model.removeJob(id);
      return new JobDeleteResponse(JobDeleteResponse.Status.OK);
    } catch (JobDoesNotExistException e) {
      throw notFound();
    } catch (JobStillDeployedException e) {
      throw badRequest(new JobDeleteResponse(JobDeleteResponse.Status.STILL_IN_USE));
    }
  }

  @Path("{id}/status")
  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Optional<JobStatus> statusGet(@PathParam("id") @Valid final JobId id)
      throws HeliosException {
    if (!id.isFullyQualified()) {
      throw badRequest("Invalid id");
    }
    return Optional.fromNullable(model.getJobStatus(id));
  }
}

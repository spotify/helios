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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.JobValidator;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.JobExistsException;
import com.spotify.helios.master.JobStillDeployedException;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import com.sun.jersey.api.core.InjectParam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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

  /**
   * Returns a {@link Map} of job id to job definition for all jobs known.  If the query
   * parameter {@code q} is specified it will only return jobs whose job id contains the string.
   */
  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Map<JobId, Job> list(@QueryParam("q") @DefaultValue("") final String q) {
    final Map<JobId, Job> allJobs = model.getJobs();

    // Return all jobs if the query string is empty
    if (q.isEmpty()) {
      metrics.jobsInJobList(allJobs.size());
      return allJobs;
    }

    // Filter jobs
    final Map<JobId, Job> filteredJobs = Maps.newHashMap();
    for (Entry<JobId, Job> entry : allJobs.entrySet()) {
      if (entry.getKey().toString().contains(q)) {
        filteredJobs.put(entry.getKey(), entry.getValue());
      }
    }

    metrics.jobsInJobList(filteredJobs.size());
    return filteredJobs;
  }


  /**
   * Returns the {@link Job} with the given id.
   */
  @Path("{id}")
  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Optional<Job> get(@InjectParam @PathParam("id") @Valid final JobId id) {
    if (!id.isFullyQualified()) {
      throw badRequest("Invalid id");
    }
    return Optional.fromNullable(model.getJob(id));
  }

  /**
   * Create a job given the definition in {@code job}.
   */
  @POST
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public CreateJobResponse post(@Valid final Job job,
                                @RequestUser final String username) {
    final Collection<String> errors = JOB_VALIDATOR.validate(job);
    final Job actualJob = job.toBuilder()
        .setCreatingUser(username)
        // if job had an id coming in, preserve it
        .setHash(job.getId().getHash())
        .build();
    final String jobIdString = actualJob.getId().toString();
    if (!errors.isEmpty()) {
      throw badRequest(new CreateJobResponse(INVALID_JOB_DEFINITION, ImmutableList.copyOf(errors),
          jobIdString));
    }
    try {
      model.addJob(actualJob);
    } catch (JobExistsException e) {
      throw badRequest(new CreateJobResponse(JOB_ALREADY_EXISTS, ImmutableList.<String>of(),
          jobIdString));
    }
    log.info("created job: {}", actualJob);
    return new CreateJobResponse(CreateJobResponse.Status.OK, ImmutableList.<String>of(),
        jobIdString);
  }

  /**
   * Deletes the job specified by the given id.
   */
  @Path("{id}")
  @DELETE
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public JobDeleteResponse delete(@PathParam("id") @Valid final JobId id,
                                  @QueryParam("token") @DefaultValue("") final String token)
          throws HeliosException
  {
    if (!id.isFullyQualified()) {
      throw badRequest("Invalid id");
    }
    try {
      model.removeJob(id);
      return new JobDeleteResponse(JobDeleteResponse.Status.OK);
    } catch (JobDoesNotExistException e) {
      throw notFound(new JobDeleteResponse(JobDeleteResponse.Status.JOB_NOT_FOUND));
    } catch (JobStillDeployedException e) {
      throw badRequest(new JobDeleteResponse(JobDeleteResponse.Status.STILL_IN_USE));
    }
  }

  /**
   * Returns the job status for the given job id.  The job status includes things like where it's
   * deployed, and the status of the jobs where it's deployed, etc.
   */
  @Path("{id}/status")
  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Optional<JobStatus> statusGet(@PathParam("id") @Valid final JobId id) {
    if (!id.isFullyQualified()) {
      throw badRequest("Invalid id");
    }
    return Optional.fromNullable(model.getJobStatus(id));
  }
  
  @Path("/statuses")
  @POST
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Map<JobId, JobStatus> jobStatuses(@Valid final Set<JobId> ids) {
    for (final JobId id : ids) {
      if (!id.isFullyQualified()) {
        throw badRequest("Invalid id " + id);
      }
    }
    final Map<JobId, JobStatus> results = Maps.newHashMap();
    for (final JobId id : ids) {
      final JobStatus status = model.getJobStatus(id);
      if (status != null) {
        results.put(id, status);
      }
    }
    return results;
  }
}

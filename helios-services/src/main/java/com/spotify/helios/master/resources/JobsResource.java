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
import com.yammer.metrics.annotation.ExceptionMetered;
import com.yammer.metrics.annotation.Timed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

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
    final Map<JobId, Job> filteredJobs = Maps.newHashMap();
    for (Entry<JobId, Job> entry : allJobs.entrySet()) {
      if (entry.getKey().toString().contains(q)) {
        filteredJobs.put(entry.getKey(), entry.getValue());
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
    final String jobIdString = job.toBuilder().build().getId().toString();
    if (!errors.isEmpty()) {
      throw badRequest(new CreateJobResponse(INVALID_JOB_DEFINITION, ImmutableList.copyOf(errors),
          jobIdString));
    }
    try {
      model.addJob(job.toBuilder().build());
    } catch (JobExistsException e) {
      throw badRequest(new CreateJobResponse(JOB_ALREADY_EXISTS, ImmutableList.<String>of(),
          jobIdString));
    }
    log.info("created job: {}", job);
    return new CreateJobResponse(CreateJobResponse.Status.OK, ImmutableList.<String>of(),
        jobIdString);
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
      throw notFound(new JobDeleteResponse(JobDeleteResponse.Status.JOB_NOT_FOUND));
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

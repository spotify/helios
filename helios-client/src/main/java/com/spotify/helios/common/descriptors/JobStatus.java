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

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.copyOf;

/**
 * Represents the status of a job.
 *
 * A typical JSON representation might be:
 * <pre>
 * {
 *   "deployments" : {
 *     "host1" : {
 *       "goal" : "START",
 *       "jobId" : "myservice:0.5:3539b7bc2235d53f79e6e8511942bbeaa8816265"
 *     },
 *     "host2" : {
 *       "goal" : "START",
 *       "jobId" : "myservice:0.5:3539b7bc2235d53f79e6e8511942bbeaa8816265"
 *     },
 *     "host3" : {
 *       "goal" : "START",
 *       "jobId" : "myservice:0.5:3539b7bc2235d53f79e6e8511942bbeaa8816265"
 *     },
 *   },
 *   "job" : { #... see definition of Job },
 *   "taskStatuses" : {
 *     "host1" : { #... see definition of TaskStatus }
 *     "host2" : { #... see definition of TaskStatus }
 *     "host3" : { #... see definition of TaskStatus }
 *   }
 * }
 * </pre>
 */
public class JobStatus {

  private final Job job;
  private final Map<String, TaskStatus> taskStatuses;
  private final Map<String, Deployment> deployments;

  public JobStatus(@JsonProperty("job") final Job job,
                   @JsonProperty("taskStatuses") final Map<String, TaskStatus> taskStatuses,
                   @JsonProperty("deployments") final Map<String, Deployment> deployments) {
    this.job = job;
    this.taskStatuses = taskStatuses;
    this.deployments = deployments;
  }

  public JobStatus(final Builder builder) {
    this.job = builder.job;
    this.taskStatuses = builder.taskStatuses;
    this.deployments = builder.deployments;
  }

  public Job getJob() {
    return job;
  }

  /** @return a map of host to task status */
  public Map<String, TaskStatus> getTaskStatuses() {
    return taskStatuses;
  }

  /** @return a map of host to deployment */
  public Map<String, Deployment> getDeployments() {
    return deployments;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final JobStatus jobStatus = (JobStatus) o;

    if (deployments != null ? !deployments.equals(jobStatus.deployments)
                            : jobStatus.deployments != null) {
      return false;
    }
    if (job != null ? !job.equals(jobStatus.job) : jobStatus.job != null) {
      return false;
    }
    if (taskStatuses != null ? !taskStatuses.equals(jobStatus.taskStatuses)
                             : jobStatus.taskStatuses != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = job != null ? job.hashCode() : 0;
    result = 31 * result + (taskStatuses != null ? taskStatuses.hashCode() : 0);
    result = 31 * result + (deployments != null ? deployments.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("job", job)
        .add("taskStatuses", taskStatuses)
        .add("deployments", deployments)
        .toString();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private Job job;
    private Map<String, TaskStatus> taskStatuses;
    public Map<String, Deployment> deployments;

    public Builder setJob(final Job job) {
      this.job = job;
      return this;
    }

    public Builder setTaskStatuses(final Map<String, TaskStatus> taskStatuses) {
      this.taskStatuses = copyOf(taskStatuses);
      return this;
    }

    public Builder setDeployments(final Map<String, Deployment> deployments) {
      this.deployments = copyOf(deployments);
      return this;
    }

    public JobStatus build() {
      return new JobStatus(this);
    }
  }
}

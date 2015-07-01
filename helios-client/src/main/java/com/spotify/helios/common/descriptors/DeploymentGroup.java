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

import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Represents a Helios deployment group.
 *
 * An sample expression of it in JSON might be:
 * <pre>
 * {
 *   "name" : "foo-group",
 *   "labels" : {
 *     "role" : "foo",
 *     "canary" : "0"
 *   },
 *   "job": "foo-service:0.1.0"
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeploymentGroup extends Descriptor {

  public static final String EMPTY_NAME = "";
  public static final Map<String, String> EMPTY_LABELS = emptyMap();
  public static final String EMPTY_JOB = "";

  private final String name;
  private final Map<String, String> labels;
  private final String job;

  /**
   * Create a Job.
   *
   * @param name The docker name to use.
   * @param job The job for the deployment group.
   * @param labels The labels that are selected for the deployment group.
   */
  public DeploymentGroup(@JsonProperty("name") final String name,
                         @JsonProperty("labels") final Map<String, String> labels,
                         @JsonProperty("job") @Nullable final String job) {
    this.name = name;
    this.labels = labels;

    // Optional
    this.job = Optional.fromNullable(job).or(EMPTY_JOB);
  }

  public String getName() {
    return name;
  }

  public String getJob() {
    return job;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DeploymentGroup that = (DeploymentGroup) o;

    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (labels != null ? !labels.equals(that.labels) : that.labels != null) {
      return false;
    }
    return !(job != null ? !job.equals(that.job) : that.job != null);

  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (labels != null ? labels.hashCode() : 0);
    result = 31 * result + (job != null ? job.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DeploymentGroup{" +
           "name='" + name + '\'' +
           ", labels=" + labels +
           ", job='" + job + '\'' +
           '}';
  }

  public Builder toBuilder() {
    final Builder builder = newBuilder();

    return builder.setName(name)
        .setJob(job)
        .setLabels(labels);
  }

  public static class Builder implements Cloneable {

    private final Parameters p;

    private Builder() {
      this.p = new Parameters();
    }

    private static class Parameters implements Cloneable {

      public String name;
      public String job;
      public Map<String, String> labels;

      private Parameters() {
        this.name = EMPTY_NAME;
        this.job = EMPTY_JOB;
        this.labels = Maps.newHashMap(EMPTY_LABELS);
      }
    }

    public Builder setName(final String name) {
      p.name = name;
      return this;
    }

    public Builder setJob(final String job) {
      p.job = job;
      return this;
    }

    public Builder setLabels(final Map<String, String> labels) {
      p.labels = Maps.newHashMap(labels);
      return this;
    }

    public String getName() {
      return p.name;
    }

    public DeploymentGroup build() {
      return new DeploymentGroup(p.name, p.labels, p.job);
    }
  }

}

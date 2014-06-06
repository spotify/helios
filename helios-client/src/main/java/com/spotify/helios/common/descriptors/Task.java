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

import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class Task extends Descriptor {

  private final Job job;
  private final Goal goal;

  public Task(@JsonProperty("job") final Job job,
              @JsonProperty("goal") final Goal goal) {
    this.job = checkNotNull(job, "job");
    this.goal = checkNotNull(goal, "goal");
  }

  public Goal getGoal() {
    return goal;
  }

  public Job getJob() {
    return job;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Task task = (Task) o;

    if (goal != task.goal) {
      return false;
    }
    if (job != null ? !job.equals(task.job) : task.job != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = goal != null ? goal.hashCode() : 0;
    result = 31 * result + (job != null ? job.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Task{" +
           "job=" + job +
           ", goal=" + goal +
           '}';
  }
}

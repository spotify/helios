/*-
 * -\-\-
 * Helios Client
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

package com.spotify.helios.common.descriptors;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * The state of the deployed job (aka a task).
 *
 * <p>A typical JSON representation might be:
 * <pre>
 * {
 *   "containerId" : "e890d827e802934a29c97d7e9e3c96a55ca049e519ab0c28be8020621a0a3750",
 *   "env" : {
 *     "SYSLOG_HOST_PORT" : "10.99.0.1:514"
 *   },
 *   "goal" : "START",
 *   "job" : { #... see the definition of Job },
 *   "ports" : {
 *     "http" : {
 *       "externalPort" : 8080,
 *       "internalPort" : 8080,
 *       "protocol" : "tcp"
 *     },
 *     "http-admin" : {
 *       "externalPort" : 8081,
 *       "internalPort" : 8081,
 *       "protocol" : "tcp"
 *     }
 *   },
 *   "state" : "RUNNING",
 *   "throttled" : "NO",
 *   "containerError": "Something broke starting the container!"
 * },
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskStatus extends Descriptor {

  private static final Map<String, PortMapping> EMPTY_PORTS = emptyMap();

  public enum State {
    PULLING_IMAGE,
    CREATING,
    STARTING,
    HEALTHCHECKING,
    RUNNING,
    EXITED,
    STOPPING,
    STOPPED,
    FAILED,
    UNKNOWN
  }

  private final Job job;
  private final Goal goal;
  private final State state;
  private final String containerId;
  private final ThrottleState throttled;
  private final Map<String, PortMapping> ports;
  private final Map<String, String> env;
  private final String containerError;

  /**
   * @param job            The job the task is running.
   * @param goal           The desired state of the task.
   * @param state          The state of the task.
   * @param containerId    The containerId, if the task has one (yet).
   * @param throttled      The throttle state of the task.
   * @param ports          The ports actually assigned to the task.
   * @param env            The environment passed to the container.
   * @param containerError The last Docker error encountered while starting the container.
   */
  public TaskStatus(@JsonProperty("job") final Job job,
                    @Nullable @JsonProperty("goal") final Goal goal,
                    @JsonProperty("state") final State state,
                    @Nullable @JsonProperty("containerId") final String containerId,
                    @JsonProperty("throttled") final ThrottleState throttled,
                    @JsonProperty("ports") final Map<String, PortMapping> ports,
                    @Nullable @JsonProperty("env") final Map<String, String> env,
                    @Nullable @JsonProperty("containerError") final String containerError) {
    this.job = checkNotNull(job, "job");
    this.goal = goal; // TODO (dano): add null check when all masters are upgraded
    this.state = checkNotNull(state, "state");

    // Optional
    this.containerId = containerId;
    this.throttled = Optional.fromNullable(throttled).or(ThrottleState.NO);
    this.ports = Optional.fromNullable(ports).or(EMPTY_PORTS);
    this.env = Optional.fromNullable(env).or(Maps.<String, String>newHashMap());
    this.containerError = Optional.fromNullable(containerError).or("");
  }

  public Builder asBuilder() {
    return newBuilder()
        .setJob(job)
        .setGoal(goal)
        .setState(state)
        .setContainerId(containerId)
        .setThrottled(throttled)
        .setPorts(ports)
        .setEnv(env)
        .setContainerError(containerError);
  }

  private TaskStatus(final Builder builder) {
    this.job = checkNotNull(builder.job, "job");
    this.goal = checkNotNull(builder.goal, "goal");
    this.state = checkNotNull(builder.state, "state");

    // Optional
    this.containerId = builder.containerId;
    this.throttled = Optional.fromNullable(builder.throttled).or(ThrottleState.NO);
    this.ports = Optional.fromNullable(builder.ports).or(EMPTY_PORTS);
    this.env = Optional.fromNullable(builder.env).or(Maps.<String, String>newHashMap());
    this.containerError = Optional.fromNullable(builder.containerError).or("");
  }

  public ThrottleState getThrottled() {
    return throttled;
  }

  @Nullable
  public String getContainerId() {
    return containerId;
  }

  public Goal getGoal() {
    return goal;
  }

  public State getState() {
    return state;
  }

  public Job getJob() {
    return job;
  }

  public Map<String, PortMapping> getPorts() {
    return ports;
  }

  public Map<String, String> getEnv() {
    return env;
  }

  public String getContainerError() {
    return containerError;
  }

  @Override
  public String toString() {
    return "TaskStatus{"
           + "job=" + job
           + ", goal=" + goal
           + ", state=" + state
           + ", containerId='" + containerId + '\''
           + ", throttled=" + throttled
           + ", ports=" + ports
           + ", env=" + env
           + ", containerError='" + containerError + '\''
           + '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final TaskStatus that = (TaskStatus) obj;

    if (containerId != null ? !containerId.equals(that.containerId) : that.containerId != null) {
      return false;
    }
    if (env != null ? !env.equals(that.env) : that.env != null) {
      return false;
    }
    if (goal != that.goal) {
      return false;
    }
    if (job != null ? !job.equals(that.job) : that.job != null) {
      return false;
    }
    if (ports != null ? !ports.equals(that.ports) : that.ports != null) {
      return false;
    }
    if (state != that.state) {
      return false;
    }
    if (throttled != that.throttled) {
      return false;
    }
    if (containerError != null ? !containerError.equals(that.containerError) :
        that.containerError != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = job != null ? job.hashCode() : 0;
    result = 31 * result + (goal != null ? goal.hashCode() : 0);
    result = 31 * result + (state != null ? state.hashCode() : 0);
    result = 31 * result + (containerId != null ? containerId.hashCode() : 0);
    result = 31 * result + (throttled != null ? throttled.hashCode() : 0);
    result = 31 * result + (ports != null ? ports.hashCode() : 0);
    result = 31 * result + (env != null ? env.hashCode() : 0);
    result = 31 * result + (containerError != null ? containerError.hashCode() : 0);
    return result;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    Builder() {}

    private Job job;
    private Goal goal;
    private State state;
    private String containerId;
    private Map<String, PortMapping> ports;
    private ThrottleState throttled;
    private Map<String, String> env;
    private String containerError;

    public Builder setJob(final Job job) {
      this.job = job;
      return this;
    }

    public Builder setGoal(Goal goal) {
      this.goal = goal;
      return this;
    }

    public Builder setState(final State state) {
      this.state = state;
      return this;
    }

    public Builder setContainerId(final String containerId) {
      this.containerId = containerId;
      return this;
    }

    public Builder setPorts(final Map<String, PortMapping> ports) {
      this.ports = ports;
      return this;
    }

    public Builder setThrottled(final ThrottleState throttled) {
      this.throttled = throttled;
      return this;
    }

    public Builder setEnv(final Map<String, String> env) {
      this.env = env;
      return this;
    }

    public Builder setContainerError(final String containerError) {
      this.containerError = containerError;
      return this;
    }

    public TaskStatus build() {
      return new TaskStatus(this);
    }
  }
}

/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.jetbrains.annotations.Nullable;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.emptyMap;

public class TaskStatus extends Descriptor {

  private static final Map<String, PortMapping> EMPTY_PORTS = emptyMap();

  public enum State {
    CREATING,
    STARTING,
    RUNNING,
    EXITED,
    STOPPED,
    FAILED,
    UNKNOWN
  }

  private final Job job;
  private final State state;
  private final String containerId;
  private final ThrottleState throttled;
  private final Map<String, PortMapping> ports;
  private final Map<String, String> env;

  public TaskStatus(@JsonProperty("job") final Job job,
                    @JsonProperty("state") final State state,
                    @Nullable @JsonProperty("containerId") final String containerId,
                    @JsonProperty("throttled") final ThrottleState throttled,
                    @JsonProperty("ports") final Map<String, PortMapping> ports,
                    @Nullable @JsonProperty("env") final Map<String, String> env) {
    this.job = checkNotNull(job, "job");
    this.state = checkNotNull(state, "state");

    // Optional
    this.containerId = containerId;
    this.throttled = Optional.fromNullable(throttled).or(ThrottleState.NO);
    this.ports = Optional.fromNullable(ports).or(EMPTY_PORTS);
    this.env = Optional.fromNullable(env).or(Maps.<String, String>newHashMap());
  }

  public Builder asBuilder() {
    return newBuilder()
        .setJob(job)
        .setState(state)
        .setContainerId(containerId)
        .setThrottled(throttled)
        .setPorts(ports)
        .setEnv(env);
  }

  private TaskStatus(final Builder builder) {
    this(builder.job, builder.state, builder.containerId, builder.throttled, builder.ports,
        builder.env);
  }

  public ThrottleState getThrottled() {
    return throttled;
  }

  @Nullable
  public String getContainerId() {
    return containerId;
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

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("job", job)
        .add("state", state)
        .add("containerId", containerId)
        .add("throttled", throttled)
        .add("ports", ports)
        .add("env", env)
        .toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final TaskStatus that = (TaskStatus) o;

    if (containerId != null ? !containerId.equals(that.containerId) : that.containerId != null) {
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
    if (!env.equals(that.env)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = job != null ? job.hashCode() : 0;
    result = 31 * result + (state != null ? state.hashCode() : 0);
    result = 31 * result + (containerId != null ? containerId.hashCode() : 0);
    result = 31 * result + (throttled != null ? throttled.hashCode() : 0);
    result = 31 * result + (ports != null ? ports.hashCode() : 0);
    result = 31 * result + (env != null ? env.hashCode() : 0);
    return result;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    Builder() {}

    private Job job;
    private State state;
    private String containerId;
    private Map<String, PortMapping> ports;
    private ThrottleState throttled;
    private Map<String, String> env;

    public Builder setEnv(final Map<String, String> env) {
      this.env = env;
      return this;
    }

    public Builder setJob(final Job job) {
      this.job = job;
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

    public TaskStatus build() {
      return new TaskStatus(this);
    }
  }
}

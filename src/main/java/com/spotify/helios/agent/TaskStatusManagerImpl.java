package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.ThrottleState;

import java.util.Map;

public class TaskStatusManagerImpl implements TaskStatusManager {
  private final AgentModel model;
  private final JobId jobId;
  private final Job job;

  private boolean isFlapping;
  private State status;
  private TaskStatus taskStatus;
  private ThrottleState throttle;

  public TaskStatusManagerImpl(AgentModel model, JobId jobId, Job job) {
    this.model = model;
    this.jobId = jobId;
    this.job = job;
  }

  @Override
  public void updateFlappingState(boolean isFlapping) {
    if (isFlapping == this.isFlapping) {
      return;
    }

    this.isFlapping = isFlapping;
    if (this.isFlapping && throttle == ThrottleState.NO) {
      throttle = ThrottleState.FLAPPING;
    } else if (throttle == ThrottleState.FLAPPING && isFlapping == false) {
      throttle = ThrottleState.NO;
    }

    updateModelStatus(taskStatus.asBuilder());
  }

  @Override
  public boolean isFlapping() {
    return isFlapping;
  }

  @Override
  public void setStatus(State status, ThrottleState throttle, String containerId,
                        Map<String, PortMapping> ports, Map<String, String> env) {

    this.throttle = throttle;
    this.status = status;

    TaskStatus.Builder builder = TaskStatus.newBuilder()
        .setJob(job)
        .setState(status)
        .setContainerId(containerId)
        .setPorts(ports)
        .setEnv(env);

    updateModelStatus(builder);
  }

  private void updateModelStatus(TaskStatus.Builder builder) {
    builder.setThrottled(throttle);
    model.setTaskStatus(jobId, builder.build());
    taskStatus = builder.build();
  }

  @Override
  public State getStatus() {
    return status;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private AgentModel model;
    private JobId jobId;
    private Job job;

    private Builder() {}

    public Builder setJobId(JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setModel(AgentModel model) {
      this.model = model;
      return this;
    }

    public Builder setJob(Job job) {
      this.job = job;
      return this;
    }

    public TaskStatusManagerImpl build() {
      return new TaskStatusManagerImpl(model, jobId, job);
    }
  }
}

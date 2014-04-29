package com.spotify.helios.testing;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.junit.rules.ExternalResource;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

public class HeliosRule extends ExternalResource {

  private final HeliosClient client;
  private final String host;

  private final Job job;

  private TaskStatus status;

  public HeliosRule(final Builder builder) {
    this.client = checkNotNull(builder.client == null ? builder.clientBuilder.build()
                                                      : builder.client, "client");
    this.job = builder.jobBuilder.build();
    this.host = checkNotNull(builder.host, "host");
  }


  public TaskStatus getStatus() {
    return status;
  }

  public Integer getPort(final String name) {
    checkNotNull(status, "status is null, job is not running");
    return status.getPorts().get(name).getExternalPort();
  }

  @Override
  protected void before() throws Throwable {
    final CreateJobResponse createJobResponse = client.createJob(job).get(30, SECONDS);
    if (createJobResponse.getStatus() != CreateJobResponse.Status.OK) {
      fail(format("Failed to create job %s - %s",
                  job.toString(), createJobResponse.toString()));
    }

    final JobId jobId = JobId.fromString(createJobResponse.getId());
    final Deployment deployment = Deployment.of(jobId, Goal.START);
    final JobDeployResponse deployResponse = client.deploy(deployment, host).get(30, SECONDS);
    if (deployResponse.getStatus() != JobDeployResponse.Status.OK) {
      fail(format("Failed to deploy job %s %s - %s",
                  jobId.toString(), job.toString(), deployResponse.toString()));
    }

    this.status = awaitUp(jobId, host);
  }

  private TaskStatus awaitUp(final JobId jobId, final String host) throws TimeoutException {
    return Polling.awaitUnchecked(1, MINUTES, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final JobStatus status = getUnchecked(client.jobStatus(jobId));
        if (status == null) {
          return null;
        }
        final TaskStatus taskStatus = status.getTaskStatuses().get(host);
        if (taskStatus == null) {
          return null;
        }

        return taskStatus.getState() == TaskStatus.State.RUNNING ? taskStatus : null;
      }
    });
  }

  @Override
  protected void after() {
    final JobUndeployResponse undeployResponse = getUnchecked(client.undeploy(job.getId(), host));
    if (undeployResponse.getStatus() != JobUndeployResponse.Status.OK &&
        undeployResponse.getStatus() != JobUndeployResponse.Status.JOB_NOT_FOUND) {
      fail(format("Failed to undeploy job %s - %s",
                  job.getId().toString(), undeployResponse.toString()));
    }

    final JobDeleteResponse deleteResponse = getUnchecked(client.deleteJob(job.getId()));
    if (deleteResponse.getStatus() != JobDeleteResponse.Status.OK) {
      fail(format("Failed to delete job %s - %s",
                  job.getId().toString(), deleteResponse.toString()));
    }

  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private final HeliosClient.Builder clientBuilder = HeliosClient.newBuilder()
        .setUser(System.getProperty("user.name"));

    private HeliosClient client;
    private String host;

    private final Job.Builder jobBuilder = Job.newBuilder();


    public Builder setDomain(final String domain) {
      this.clientBuilder.setDomain(domain);
      return this;
    }

    public Builder setUser(final String user) {
      this.clientBuilder.setUser(user);
      return this;
    }

    public Builder setClient(HeliosClient client) {
      this.client = client;
      return this;
    }

    public Builder setJobName(String jobName) {
      this.jobBuilder.setName(jobName);
      return this;
    }

    public Builder setJobVersion(String jobVersion) {
      this.jobBuilder.setVersion(jobVersion);
      return this;
    }

    public Builder setImageName(String imageName) {
      this.jobBuilder.setImage(imageName);
      return this;
    }

    public Builder setCommand(List<String> command) {
      this.jobBuilder.setCommand(command);
      return this;
    }

    public Builder addPort(String name, int internalPort) {
      this.jobBuilder.addPort(name, PortMapping.of(internalPort));
      return this;
    }

    public Builder addPort(String name, int internalPort, int externalPort) {
      this.jobBuilder.addPort(name, PortMapping.of(internalPort, externalPort));
      return this;
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public HeliosRule build() {
      return new HeliosRule(this);
    }
  }
}

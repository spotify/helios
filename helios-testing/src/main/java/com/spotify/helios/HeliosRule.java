package com.spotify.helios;

import com.google.common.collect.ImmutableMap;

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
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

public class HeliosRule extends ExternalResource {

  private final HeliosClient heliosClient;
  private final String jobName;
  private final String jobVersion;
  private final String imageName;
  private final List<String> command;
  private final String host;
  private final String portName;
  private final int internalPort;
  private final Integer externalPort;

  private TaskStatus status;

  private HeliosRule(HeliosClient heliosClient, String jobName, String jobVersion, String imageName,
                    List<String> command, String host, String portName, int internalPort,
                    Integer externalPort) {
    this.heliosClient = heliosClient;
    this.jobName = jobName;
    this.jobVersion = jobVersion;
    this.imageName = imageName;
    this.command = command;
    this.host = host;
    this.portName = portName;
    this.internalPort = internalPort;
    this.externalPort = externalPort;
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
    final Job job = Job.newBuilder()
        .setName(jobName)
        .setVersion(jobVersion)
        .setImage(imageName)
        .setCommand(command)
        .setPorts(ImmutableMap.of(portName, PortMapping.of(internalPort, externalPort)))
        .build();

    final CreateJobResponse createJobResponse = heliosClient.createJob(job).get(30, SECONDS);
    if (createJobResponse.getStatus() != CreateJobResponse.Status.OK) {
      fail(format("Failed to create job %s - %s",
                  job.toString(), createJobResponse.toString()));
    }

    final JobId jobId = JobId.fromString(createJobResponse.getId());
    final Deployment deployment = Deployment.of(jobId, Goal.START);
    final JobDeployResponse deployResponse = heliosClient.deploy(deployment, host).get(30, SECONDS);
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
        final JobStatus status = getUnchecked(heliosClient.jobStatus(jobId));
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
    final String query = format("%s:%s", jobName, jobVersion);
    final Map<JobId, Job> jobs = getUnchecked(heliosClient.jobs(query));

    if (jobs.isEmpty()) {
      return;
    }

    final JobId jobId = getOnlyElement(jobs.keySet());
    final JobUndeployResponse undeployResponse = getUnchecked(heliosClient.undeploy(jobId, host));
    if (undeployResponse.getStatus() != JobUndeployResponse.Status.OK &&
        undeployResponse.getStatus() != JobUndeployResponse.Status.JOB_NOT_FOUND) {
      fail(format("Failed to undeploy job %s - %s",
                  jobId.toString(), undeployResponse.toString()));
    }

    final JobDeleteResponse deleteResponse = getUnchecked(heliosClient.deleteJob(jobId));
    if (deleteResponse.getStatus() != JobDeleteResponse.Status.OK) {
      fail(format("Failed to delete job %s - %s",
                  jobId.toString(), deleteResponse.toString()));
    }

  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private HeliosClient heliosClient;
    private String jobName;
    private String jobVersion;
    private String imageName;
    private List<String> command = emptyList();
    private String host;
    private String portName;
    private int internalPort;
    private Integer externalPort;

    public Builder setHeliosClient(HeliosClient heliosClient) {
      this.heliosClient = heliosClient;
      return this;
    }

    public Builder setJobName(String jobName) {
      this.jobName = jobName;
      return this;
    }

    public Builder setJobVersion(String jobVersion) {
      this.jobVersion = jobVersion;
      return this;
    }

    public Builder setImageName(String imageName) {
      this.imageName = imageName;
      return this;
    }

    public Builder setCommand(List<String> command) {
      this.command = command;
      return this;
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setPortName(String portName) {
      this.portName = portName;
      return this;
    }

    public Builder setInternalPort(int internalPort) {
      this.internalPort = internalPort;
      return this;
    }

    public Builder setExternalPort(int externalPort) {
      this.externalPort = externalPort;
      return this;
    }

    public HeliosRule build() {
      return new HeliosRule(heliosClient, jobName, jobVersion, imageName, command, host, portName,
                            internalPort, externalPort);
    }

  }
}

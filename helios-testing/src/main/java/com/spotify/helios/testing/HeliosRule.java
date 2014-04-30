package com.spotify.helios.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.junit.rules.ExternalResource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

public class HeliosRule extends ExternalResource {

  private final HeliosClient client;
  private final List<String> hosts;

  private final Job job;

  private Map<String, TaskStatus> statuses = Maps.newHashMap();

  public HeliosRule(final Builder builder) {
    this.client = checkNotNull(builder.client == null ? builder.clientBuilder.build()
                                                      : builder.client, "client");
    final Job.Builder b = builder.jobBuilder.clone();
    if (b.getName() == null && b.getVersion() == null) {
      // Both name and version are unset, use image name as job name and generate random version
      b.setName(jobName(b.getImage()));
      b.setVersion(randomVersion());
    }
    this.job = b.build();
    this.hosts = ImmutableList.copyOf(checkNotNull(builder.hosts, "hosts"));
  }

  private String jobName(final String s) {
    return "test_" + s.replace(':', '_');
  }

  private String randomVersion() {
    final byte[] versionBytes = new byte[8];
    ThreadLocalRandom.current().nextBytes(versionBytes);
    return BaseEncoding.base16().encode(versionBytes);
  }

  public TaskStatus getStatus(final String host) {
    return statuses.get(host);
  }

  public Job getJob() {
    return job;
  }

  public Integer getPort(final String host, final String name) {
    final TaskStatus status = statuses.get(host);
    checkNotNull(status, "Status is null, job is not running on host %s", host);
    final PortMapping portMapping = status.getPorts().get(name);
    checkNotNull(portMapping, "Port %s not found", name);
    return portMapping.getExternalPort();
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
    final List<ListenableFuture<JobDeployResponse>> futures = Lists.newArrayList();
    for (final String host : hosts) {
      futures.add(client.deploy(deployment, host));
    }
    final List<JobDeployResponse> responses = Futures.allAsList(futures).get(30, SECONDS);
    for (final JobDeployResponse response : responses) {
      if (response.getStatus() != JobDeployResponse.Status.OK) {
        fail(format("Failed to deploy job %s %s - %s",
                    jobId.toString(), job.toString(), response.toString()));
      }
    }
    for (final String host : hosts) {
      statuses.put(host, awaitUp(jobId, host));
    }
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
    final List<AssertionError> errors = Lists.newArrayList();

    // Undeploy job
    final List<ListenableFuture<JobUndeployResponse>> futures = Lists.newArrayList();
    for (final String host : hosts) {
      futures.add(client.undeploy(job.getId(), host));
    }
    for (final ListenableFuture<JobUndeployResponse> future : futures) {
      try {
        final JobUndeployResponse response = future.get(30, SECONDS);
        if (response.getStatus() != JobUndeployResponse.Status.OK &&
            response.getStatus() != JobUndeployResponse.Status.JOB_NOT_FOUND) {
          errors.add(new AssertionError(format("Failed to undeploy job %s - %s",
                                               job.getId().toString(), response.toString())));
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        errors.add(new AssertionError(e));
      }
    }

    // Delete job
    final JobDeleteResponse deleteResponse = getUnchecked(client.deleteJob(job.getId()));
    if (deleteResponse.getStatus() != JobDeleteResponse.Status.OK) {
      errors.add(new AssertionError(format("Failed to delete job %s - %s",
                                           job.getId().toString(), deleteResponse.toString())));
    }

    // Raise any errors
    if (!errors.isEmpty()) {
      throw errors.get(0);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private final HeliosClient.Builder clientBuilder = HeliosClient.newBuilder()
        .setUser(System.getProperty("user.name"));
    private HeliosClient client;
    private final List<String> hosts = Lists.newArrayList();
    private final Job.Builder jobBuilder = Job.newBuilder();

    public Builder domain(final String domain) {
      this.clientBuilder.setDomain(domain);
      return this;
    }

    public Builder user(final String user) {
      this.clientBuilder.setUser(user);
      return this;
    }

    public Builder client(HeliosClient client) {
      this.client = client;
      return this;
    }

    public Builder name(String jobName) {
      this.jobBuilder.setName(jobName);
      return this;
    }

    public Builder version(String jobVersion) {
      this.jobBuilder.setVersion(jobVersion);
      return this;
    }

    public Builder image(String image) {
      this.jobBuilder.setImage(image);
      return this;
    }

    public Builder command(List<String> command) {
      this.jobBuilder.setCommand(command);
      return this;
    }

    public Builder command(final String... command) {
      return command(asList(command));
    }

    public Builder port(String name, int internalPort) {
      this.jobBuilder.addPort(name, PortMapping.of(internalPort));
      return this;
    }

    public Builder port(String name, int internalPort, int externalPort) {
      this.jobBuilder.addPort(name, PortMapping.of(internalPort, externalPort));
      return this;
    }

    public Builder registration(ServiceEndpoint endpoint, ServicePorts ports) {
      this.jobBuilder.addRegistration(endpoint, ports);
      return this;
    }

    public Builder registration(final String service, final String protocol, final String... ports) {
      return registration(ServiceEndpoint.of(service, protocol), ServicePorts.of(ports));
    }

    public Builder registration(final Map<ServiceEndpoint, ServicePorts> registration) {
      this.jobBuilder.setRegistration(registration);
      return this;
    }

    public Builder host(String host) {
      this.hosts.add(host);
      return this;
    }

    public HeliosRule build() {
      return new HeliosRule(this);
    }
  }
}

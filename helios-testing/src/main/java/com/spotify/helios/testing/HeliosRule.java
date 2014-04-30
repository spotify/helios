package com.spotify.helios.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

public class HeliosRule extends ExternalResource {

  private final HeliosClient client;

  private final List<TemporaryJob.Builder> builders = Lists.newArrayList();
  private final Map<JobId, TemporaryJob> jobs = Maps.newLinkedHashMap();

  private final long timeoutMillis = TimeUnit.MINUTES.toMillis(5);

  public HeliosRule(final HeliosClient client) {
    this.client = client;
  }

  public TemporaryJob.Builder job() {
    final TemporaryJob.Builder builder = new TemporaryJob.Builder();
    builders.add(builder);
    return builder;
  }

  public static HeliosRule forDomain(final String domain) {
    return new HeliosRule(HeliosClient.create(domain, System.getProperty("user.name")));
  }

  @Override
  protected void before() throws Throwable {
    for (final TemporaryJob.Builder builder : builders) {
      final TemporaryJob job = builder.build();
      jobs.put(job.getJob().getId(), job);
    }

    // Create jobs
    final Map<JobId, ListenableFuture<CreateJobResponse>> createFutures = Maps.newLinkedHashMap();
    for (Map.Entry<JobId, TemporaryJob> entry : jobs.entrySet()) {
      createFutures.put(entry.getKey(), client.createJob(entry.getValue().job));
    }
    for (Map.Entry<JobId, ListenableFuture<CreateJobResponse>> entry : createFutures.entrySet()) {
      final ListenableFuture<CreateJobResponse> future = entry.getValue();
      final CreateJobResponse response = future.get(timeoutMillis, MILLISECONDS);
      if (response.getStatus() != CreateJobResponse.Status.OK) {
        final Job job = jobs.get(entry.getKey()).job;
        fail(format("Failed to create job %s - %s", job.toString(), response.toString()));
      }
    }

    // Deploy jobs
    final Map<JobId, List<ListenableFuture<JobDeployResponse>>> deployFutures =
        Maps.newLinkedHashMap();
    for (Map.Entry<JobId, TemporaryJob> entry : jobs.entrySet()) {
      final List<ListenableFuture<JobDeployResponse>> futures = Lists.newArrayList();
      deployFutures.put(entry.getKey(), futures);
      final TemporaryJob job = entry.getValue();
      final Deployment deployment = Deployment.of(job.getJob().getId(), Goal.START);
      for (final String host : job.hosts) {
        futures.add(client.deploy(deployment, host));
      }
    }
    for (Map.Entry<JobId, List<ListenableFuture<JobDeployResponse>>> entry :
        deployFutures.entrySet()) {
      final TemporaryJob job = jobs.get(entry.getKey());
      final List<ListenableFuture<JobDeployResponse>> futures = entry.getValue();
      final List<JobDeployResponse> responses = allAsList(futures).get(timeoutMillis, MILLISECONDS);
      for (final JobDeployResponse response : responses) {
        if (response.getStatus() != JobDeployResponse.Status.OK) {
          fail(format("Failed to deploy job %s %s - %s",
                      job.getJob().getId(), job.getJob().toString(), response));
        }
      }
      for (final String host : job.hosts) {
        job.statuses.put(host, awaitUp(job.getJob().getId(), host));
      }
    }
  }


  private TaskStatus awaitUp(final JobId jobId, final String host) throws TimeoutException {
    return Polling.awaitUnchecked(timeoutMillis, MILLISECONDS, new Callable<TaskStatus>() {
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

    // Undeploy jobs
    final Map<JobId, List<ListenableFuture<JobUndeployResponse>>> undeployFutures =
        Maps.newHashMap();
    for (final Map.Entry<JobId, TemporaryJob> entry : jobs.entrySet()) {
      final List<ListenableFuture<JobUndeployResponse>> futures = Lists.newArrayList();
      undeployFutures.put(entry.getKey(), futures);
      final TemporaryJob job = entry.getValue();
      for (final String host : job.hosts) {
        futures.add(client.undeploy(job.getJob().getId(), host));
      }
    }
    for (final Map.Entry<JobId, List<ListenableFuture<JobUndeployResponse>>> entry :
        undeployFutures.entrySet()) {
      final TemporaryJob job = jobs.get(entry.getKey());
      final List<ListenableFuture<JobUndeployResponse>> futures = entry.getValue();
      for (final ListenableFuture<JobUndeployResponse> future : futures) {
        try {
          final JobUndeployResponse response = future.get(timeoutMillis, MILLISECONDS);
          if (response.getStatus() != JobUndeployResponse.Status.OK &&
              response.getStatus() != JobUndeployResponse.Status.JOB_NOT_FOUND) {
            errors.add(new AssertionError(format("Failed to undeploy job %s - %s",
                                                 job.getJob().getId(), response)));
          }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          errors.add(new AssertionError(e));
        }
      }
    }

    // Delete jobs
    final Map<JobId, ListenableFuture<JobDeleteResponse>> deleteFutures = Maps.newHashMap();
    for (final Map.Entry<JobId, TemporaryJob> entry : jobs.entrySet()) {
      final TemporaryJob job = jobs.get(entry.getKey());
      deleteFutures.put(entry.getKey(), client.deleteJob(job.getJob().getId()));
    }
    for (Map.Entry<JobId, ListenableFuture<JobDeleteResponse>> entry : deleteFutures.entrySet()) {
      final TemporaryJob job = jobs.get(entry.getKey());
      final ListenableFuture<JobDeleteResponse> future = entry.getValue();
      final JobDeleteResponse response;
      try {
        response = future.get(timeoutMillis, MILLISECONDS);
        if (response.getStatus() != JobDeleteResponse.Status.OK) {
          errors.add(new AssertionError(format("Failed to delete job %s - %s",
                                               job.getJob().getId().toString(), response.toString())));
        }
      } catch (Exception e) {
        errors.add(new AssertionError(e));
      }
    }

    // Raise any errors
    if (!errors.isEmpty()) {
      throw errors.get(0);
    }
  }

  public static class TemporaryJob {

    private final Job job;
    private final List<String> hosts;

    private final Map<String, TaskStatus> statuses = Maps.newHashMap();

    private TemporaryJob(final Builder builder) {
      final Job.Builder b = builder.jobBuilder.clone();
      b.setHash(null);
      if (b.getName() == null && b.getVersion() == null) {
        // Both name and version are unset, use image name as job name and generate random version
        b.setName(jobName(b.getImage()));
        b.setVersion(randomVersion());
      }
      this.job = b.build();
      this.hosts = ImmutableList.copyOf(checkNotNull(builder.hosts, "hosts"));
    }

    public Job getJob() {
      return job;
    }

    public Integer getPort(final String host, final String port) {
      final TaskStatus status = statuses.get(host);
      if (status == null) {
        return null;
      }
      final PortMapping portMapping = status.getPorts().get(port);
      if (portMapping == null) {
        return null;
      }
      return portMapping.getExternalPort();
    }

    private String jobName(final String s) {
      return "test_" + s.replace(':', '_');
    }

    private String randomVersion() {
      final byte[] versionBytes = new byte[8];
      ThreadLocalRandom.current().nextBytes(versionBytes);
      return BaseEncoding.base16().encode(versionBytes);
    }

    public static class Builder {

      private final List<String> hosts = Lists.newArrayList();
      private final Job.Builder jobBuilder = Job.newBuilder();

      private TemporaryJob job;

      public Builder name(final String jobName) {
        this.jobBuilder.setName(jobName);
        return this;
      }

      public Builder version(final String jobVersion) {
        this.jobBuilder.setVersion(jobVersion);
        return this;
      }

      public Builder image(final String image) {
        this.jobBuilder.setImage(image);
        return this;
      }

      public Builder command(final List<String> command) {
        this.jobBuilder.setCommand(command);
        return this;
      }

      public Builder command(final String... command) {
        return command(asList(command));
      }

      public Builder port(final String name, final int internalPort) {
        this.jobBuilder.addPort(name, PortMapping.of(internalPort));
        return this;
      }

      public Builder port(final String name, final int internalPort, final int externalPort) {
        this.jobBuilder.addPort(name, PortMapping.of(internalPort, externalPort));
        return this;
      }

      public Builder registration(final ServiceEndpoint endpoint, final ServicePorts ports) {
        this.jobBuilder.addRegistration(endpoint, ports);
        return this;
      }

      public Builder registration(final String service, final String protocol,
                                  final String... ports) {
        return registration(ServiceEndpoint.of(service, protocol), ServicePorts.of(ports));
      }

      public Builder registration(final Map<ServiceEndpoint, ServicePorts> registration) {
        this.jobBuilder.setRegistration(registration);
        return this;
      }

      public Builder host(final String host) {
        this.hosts.add(host);
        return this;
      }

      public TemporaryJob build() {
        if (job == null) {
          job = new TemporaryJob(this);
        }
        return job;
      }
    }
  }
}

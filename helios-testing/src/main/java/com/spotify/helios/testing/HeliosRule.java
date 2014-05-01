package com.spotify.helios.testing;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

public class HeliosRule extends ExternalResource {

  private final HeliosClient client;

  private final List<TemporaryJob.Builder> builders = Lists.newArrayList();
  private final List<TemporaryJob> jobs = Lists.newArrayList();

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
  protected void before() throws InterruptedException, ExecutionException, TimeoutException {
    for (final TemporaryJob.Builder builder : builders) {
      final TemporaryJob job = builder.build();
      jobs.add(job);
    }

    for (TemporaryJob job : jobs) {
      setUp(job);
    }
  }

  private void setUp(final TemporaryJob job)
      throws InterruptedException, ExecutionException, TimeoutException {
    job.compile();

    // Create job
    final CreateJobResponse createResponse = get(client.createJob(job.getJob()));
    if (createResponse.getStatus() != CreateJobResponse.Status.OK) {
      fail(format("Failed to create job %s - %s", job.getJob().getId(), createResponse.toString()));
    }

    // Deploy job
    final Deployment deployment = Deployment.of(job.getJob().getId(), Goal.START);
    for (final String host : job.hosts) {
      final JobDeployResponse deployResponse = get(client.deploy(deployment, host));
      if (deployResponse.getStatus() != JobDeployResponse.Status.OK) {
        fail(format("Failed to deploy job %s %s - %s",
                    job.getJob().getId(), job.getJob().toString(), deployResponse));
      }
    }

    // Wait for job to come up
    for (final String host : job.hosts) {
      job.statuses.put(host, awaitUp(job.getJob().getId(), host));
    }
  }

  private <T> T get(final ListenableFuture<T> future)
      throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(timeoutMillis, MILLISECONDS);
  }

  private TaskStatus awaitUp(final JobId jobId, final String host) throws TimeoutException {
    return Polling.awaitUnchecked(timeoutMillis, MILLISECONDS, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final JobStatus status = Futures.getUnchecked(client.jobStatus(jobId));
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

    for (final TemporaryJob job : jobs) {
      tearDown(errors, job);
    }

    // Raise any errors
    if (!errors.isEmpty()) {
      throw errors.get(0);
    }
  }

  private void tearDown(final List<AssertionError> errors, final TemporaryJob job) {
    for (String host : job.hosts) {
      final JobId jobId = job.getJob().getId();
      final JobUndeployResponse response;
      try {
        response = get(client.undeploy(jobId, host));
        if (response.getStatus() != JobUndeployResponse.Status.OK &&
            response.getStatus() != JobUndeployResponse.Status.JOB_NOT_FOUND) {
          errors.add(new AssertionError(format("Failed to undeploy job %s - %s",
                                               job.getJob().getId(), response)));
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        errors.add(new AssertionError(e));
      }
    }

    try {
      final JobDeleteResponse response = get(client.deleteJob(job.getJob().getId()));
      if (response.getStatus() != JobDeleteResponse.Status.OK) {
        errors.add(new AssertionError(format("Failed to delete job %s - %s",
                                             job.getJob().getId().toString(), response.toString())));
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      errors.add(new AssertionError(e));
    }
  }

  public static class TemporaryJob {

    private final List<String> hosts;

    private final Map<String, Map<String, SettableFuture<InetSocketAddress>>> addresses =
        Maps.newHashMap();
    private final Map<String, TaskStatus> statuses = Maps.newHashMap();

    private final Job.Builder jobBuilder;
    private final List<Object> command;
    private final Map<String, Object> env;

    private Job job;

    private TemporaryJob(final Builder builder) {
      this.jobBuilder = builder.jobBuilder.clone();
      this.command = ImmutableList.copyOf(builder.command);
      this.env = ImmutableMap.copyOf(builder.env);
      this.hosts = ImmutableList.copyOf(checkNotNull(builder.hosts, "hosts"));
      for (final Map.Entry<String, PortMapping> entry : this.jobBuilder.getPorts().entrySet()) {
        final Map<String, SettableFuture<InetSocketAddress>> futures = Maps.newHashMap();
        for (final String host : hosts) {
          futures.put(host, SettableFuture.<InetSocketAddress>create());
        }
        this.addresses.put(entry.getKey(), futures);
      }
    }

    private void compile() {
      if (jobBuilder.getName() == null && jobBuilder.getVersion() == null) {
        // Both name and version are unset, use image name as job name and generate random version
        jobBuilder.setName(jobName(jobBuilder.getImage()));
        jobBuilder.setVersion(randomVersion());
      }
      for (final Map.Entry<String, Object> entry : this.env.entrySet()) {
        jobBuilder.addEnv(entry.getKey(), value(entry.getValue()));
      }
      final List<String> command = Lists.newArrayList();
      for (final Object arg : this.command) {
        command.add(value(arg));
      }
      jobBuilder.setCommand(command);
      job = jobBuilder.build();
    }

    private String value(final Object value) {
      if (value instanceof Future) {
        return Futures.getUnchecked((Future<?>) value).toString();
      } else {
        return value.toString();
      }
    }

    public Job getJob() {
      return job;
    }

    public Integer getPort(final String host, final String port) {
      checkArgument(hosts.contains(host), "host %s not found", host);
      checkArgument(job.getPorts().containsKey(port), "port %s not found", port);
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

    public List<ListenableFuture<InetSocketAddress>> addresses(final String port) {
      checkArgument(jobBuilder.getPorts().containsKey(port), "port %s not found", port);
      return ImmutableList.<ListenableFuture<InetSocketAddress>>copyOf(
          addresses.get(port).values());
    }

    public void setStatuses(final Map<String, TaskStatus> statuses) {
      for (final Map.Entry<String, PortMapping> entry : jobBuilder.getPorts().entrySet()) {
        for (Map.Entry<String, TaskStatus> statusEntry : statuses.entrySet()) {
          final String host = statusEntry.getKey();
          final TaskStatus taskStatus = statusEntry.getValue();
          final PortMapping portMapping = taskStatus.getPorts().get(entry.getKey());
          final Integer externalPort = portMapping.getExternalPort();
          assert externalPort != null;
          final InetSocketAddress address = InetSocketAddress.createUnresolved(host, externalPort);
          addresses.get(entry.getKey()).get(host).set(address);
        }
      }
    }

    public static class Builder {

      private final List<String> hosts = Lists.newArrayList();
      private final Job.Builder jobBuilder = Job.newBuilder();

      private Map<String, Object> env = Maps.newHashMap();

      private TemporaryJob job;
      private List<Object> command;

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

      public Builder command(final List<Object> command) {
        this.command = command;
        return this;
      }

      public Builder command(final Object... command) {
        return command(asList(command));
      }

      public Builder env(final String key, final String value) {
        this.env.put(key, value);
        return this;
      }

      public Builder env(final String key, final ListenableFuture<String> value) {
        this.env.put(key, value);
        return this;
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

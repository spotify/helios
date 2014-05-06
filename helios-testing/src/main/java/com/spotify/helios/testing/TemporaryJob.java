/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.fasterxml.jackson.databind.JsonNode;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.fasterxml.jackson.databind.node.JsonNodeType.STRING;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Integer.toHexString;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

public class TemporaryJob {

  public static final Pattern JOB_NAME_FORBIDDEN_CHARS = Pattern.compile("[^0-9a-zA-Z-_.]+");
  private static final long TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);

  private final List<String> hosts;
  private final Map<String, TaskStatus> statuses = Maps.newHashMap();
  private final HeliosClient client;
  private final Job job;

  private TemporaryJob(final HeliosClient client, final Builder builder) {
    this.client = client;

    final Job.Builder jobBuilder = builder.jobBuilder.clone();
    if (jobBuilder.getName() == null && jobBuilder.getVersion() == null) {
      // Both name and version are unset, use image name as job name and generate random version
      jobBuilder.setName(jobName(jobBuilder.getImage()));
      jobBuilder.setVersion(randomVersion());
    }
    this.hosts = ImmutableList.copyOf(checkNotNull(builder.hosts, "hosts"));
    this.job = jobBuilder.build();

    deploy();
  }

  public Job job() {
    return job;
  }

  public Integer port(final String host, final String port) {
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


  public List<HostAndPort> addresses(final String port) {
    checkArgument(job.getPorts().containsKey(port), "port %s not found", port);
    final List<HostAndPort> addresses = Lists.newArrayList();
    for (Map.Entry<String, TaskStatus> entry : statuses.entrySet()) {
      final Integer externalPort = entry.getValue().getPorts().get(port).getExternalPort();
      assert externalPort != null;
      addresses.add(HostAndPort.fromParts(entry.getKey(), externalPort));
    }
    return addresses;
  }

  void deploy() {
    try {
      // Create job
      final CreateJobResponse createResponse = get(client.createJob(job));
      if (createResponse.getStatus() != CreateJobResponse.Status.OK) {
        fail(format("Failed to create job %s - %s", job.getId(),
                    createResponse.toString()));
      }

      // Deploy job
      final Deployment deployment = Deployment.of(job.getId(), Goal.START);
      for (final String host : hosts) {
        final JobDeployResponse deployResponse = get(client.deploy(deployment, host));
        if (deployResponse.getStatus() != JobDeployResponse.Status.OK) {
          fail(format("Failed to deploy job %s %s - %s",
                      job.getId(), job.toString(), deployResponse));
        }
      }

      // Wait for job to come up
      for (final String host : hosts) {
        statuses.put(host, awaitUp(job.getId(), host));
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      fail(format("Failed to deploy job %s %s - %s",
                  job.getId(), job.toString(), e));
    }
  }

  void undeploy(final List<AssertionError> errors) {
    for (String host : hosts) {
      final JobId jobId = job.getId();
      final JobUndeployResponse response;
      try {
        response = get(client.undeploy(jobId, host));
        if (response.getStatus() != JobUndeployResponse.Status.OK &&
            response.getStatus() != JobUndeployResponse.Status.JOB_NOT_FOUND) {
          errors.add(new AssertionError(format("Failed to undeploy job %s - %s",
                                               job.getId(), response)));
        }
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        errors.add(new AssertionError(e));
      }
    }

    try {
      final JobDeleteResponse response = get(client.deleteJob(job.getId()));
      if (response.getStatus() != JobDeleteResponse.Status.OK) {
        errors.add(new AssertionError(format("Failed to delete job %s - %s",
                                             job.getId().toString(), response.toString())));
      }
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      errors.add(new AssertionError(e));
    }
  }

  private TaskStatus awaitUp(final JobId jobId, final String host) throws TimeoutException {
    return Polling.awaitUnchecked(TIMEOUT_MILLIS, MILLISECONDS, new Callable<TaskStatus>() {
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

  private String jobName(final String s) {
    return "tmp_" + JOB_NAME_FORBIDDEN_CHARS.matcher(s).replaceAll("_");
  }

  private String randomVersion() {
    return toHexString(ThreadLocalRandom.current().nextInt());
  }

  private static <T> T get(final ListenableFuture<T> future)
      throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(TIMEOUT_MILLIS, MILLISECONDS);
  }

  public static class Builder {

    private final HeliosClient client;

    private final List<String> hosts = Lists.newArrayList();
    private final Job.Builder jobBuilder = Job.newBuilder();

    TemporaryJob job;

    public Builder(final HeliosClient client) {
      this.client = client;
    }

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

    public Builder env(final String key, final String value) {
      this.jobBuilder.addEnv(key, value);
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

    public TemporaryJob deploy() {
      if (job == null) {
        job = new TemporaryJob(client, this);
      }
      return job;
    }

    public Builder imageFromBuild() {
      final String envPath = getenv("IMAGE_INFO_PATH");
      if (envPath != null) {
        return imageFromInfoFile(envPath);
      } else {
        try {
          final String name = fromNullable(getenv("IMAGE_INFO_NAME")).or("image_info.json");
          final URL info = Resources.getResource(name);
          final String json = Resources.asCharSource(info, UTF_8).read();
          return imageFromInfoJson(json, info.toString());
        } catch (IOException e) {
          throw new AssertionError("Failed to load image info", e);
        }
      }
    }

    public Builder imageFromInfoFile(final Path path) {
      return imageFromInfoFile(path.toFile());
    }

    public Builder imageFromInfoFile(final String path) {
      return imageFromInfoFile(new File(path));
    }

    public Builder imageFromInfoFile(final File file) {
      final String json;
      try {
        json = Files.toString(file, UTF_8);
      } catch (IOException e) {
        throw new AssertionError("Failed to read image info file: " +
                                 file + ": " + e.getMessage());
      }
      return imageFromInfoJson(json, file.toString());
    }

    private Builder imageFromInfoJson(final String json,
                                      final String source) {
      try {
        final JsonNode info = Json.readTree(json);
        final JsonNode imageNode = info.get("image");
        if (imageNode == null) {
          fail("Missing image field in image info: " + source);
        }
        if (imageNode.getNodeType() != STRING) {
          fail("Bad image field in image info: " + source);
        }
        final String image = imageNode.asText();
        return image(image);
      } catch (IOException e) {
        throw new AssertionError("Failed to parse image info: " + source, e);
      }
    }
  }
}

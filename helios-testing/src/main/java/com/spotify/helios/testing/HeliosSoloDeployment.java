/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Collections.singletonList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerHost;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.exceptions.ImageNotFoundException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Info;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A HeliosSoloDeployment represents a deployment of Helios Solo, which is to say one Helios
 * master and one Helios agent deployed in Docker. Helios Solo uses the Docker instance it is
 * deployed on to run its jobs.
 */
public class HeliosSoloDeployment implements HeliosDeployment {

  private static final Logger log = LoggerFactory.getLogger(HeliosSoloDeployment.class);

  public static final String BOOT2DOCKER_SIGNATURE = "Boot2Docker";
  public static final String PROBE_IMAGE = "spotify/alpine:latest";
  public static final String HELIOS_NAME_SUFFIX = ".solo.local"; //  Required for SkyDNS discovery.
  public static final String HELIOS_ID_SUFFIX = "-solo-host";
  public static final String HELIOS_CONTAINER_PREFIX = "helios-solo-container-";
  public static final String HELIOS_SOLO_PROFILE = "helios.solo.profile";
  public static final String HELIOS_SOLO_PROFILES = "helios.solo.profiles.";
  public static final int HELIOS_MASTER_PORT = 5801;
  private static final int DEFAULT_WAIT_SECONDS = 30;

  private final DockerClient dockerClient;
  // The DockerHost we use to communicate with docker.
  private final DockerHost dockerHost;
  // The DockerHost the container uses to communicate with docker.
  private final DockerHost containerDockerHost;
  private final String heliosSoloImage;
  private final boolean pullBeforeCreate;
  private final String namespace;
  private final String agentName;
  private final List<String> env;
  private final List<String> binds;
  private final String heliosContainerId;
  private final HostAndPort deploymentAddress;
  private final HeliosClient heliosClient;
  private boolean removeHeliosSoloContainerOnExit;
  private final int jobUndeployWaitSeconds;

  private HeliosSoloLogService logService;

  HeliosSoloDeployment(final Builder builder) {
    this.heliosSoloImage = builder.heliosSoloImage;
    this.pullBeforeCreate = builder.pullBeforeCreate;
    this.removeHeliosSoloContainerOnExit = builder.removeHeliosSoloContainerOnExit;
    this.jobUndeployWaitSeconds = builder.jobUndeployWaitSeconds;

    final String username = Optional.fromNullable(builder.heliosUsername).or(randomString());

    this.dockerClient = checkNotNull(builder.dockerClient, "dockerClient");
    this.dockerHost = Optional.fromNullable(builder.dockerHost).or(DockerHost.fromEnv());

    final Info dockerInfo;
    try {
      dockerInfo = this.dockerClient.info();
    } catch (DockerException | InterruptedException e1) {
      // There's not a lot we can do if Docker is unreachable.
      throw new RuntimeException(e1);
    }
    this.containerDockerHost = Optional.fromNullable(builder.containerDockerHost)
        .or(containerDockerHost(dockerInfo));

    this.namespace = Optional.fromNullable(builder.namespace).or(randomString());
    this.agentName = this.namespace + HELIOS_NAME_SUFFIX;
    this.env = buildContainerEnvironmentVariables(builder);
    this.binds = buildContainerBinds(builder);

    final String heliosPort;
    try {
      final String heliosHost = determineHeliosHost(dockerInfo);
      this.heliosContainerId = deploySolo(heliosHost);
      heliosPort = getHostPort(this.heliosContainerId, HELIOS_MASTER_PORT);
    } catch (HeliosDeploymentException e) {
      throw new AssertionError("Unable to deploy helios-solo container.", e);
    }

    // Running the String host:port through HostAndPort does some validation for us.
    this.deploymentAddress = HostAndPort.fromString(dockerHost.address() + ":" + heliosPort);
    this.heliosClient = Optional.fromNullable(builder.heliosClient).or(
        HeliosClient.newBuilder()
            .setUser(username)
            .setEndpoints("http://" + deploymentAddress)
            .build());

    if (builder.logStreamFollower != null) {
      logService = new HeliosSoloLogService(heliosClient, dockerClient, builder.logStreamFollower);
      logService.startAsync().awaitRunning();
    }
  }

  /**
   * Determine what address to use when attempting to communicate with containers deployed via the
   * helios-solo container. This will be passed into the helios-solo container as the HOST_ADDRESS
   * environment variable and is later used by TemporaryJob to figure out how to reach ports mapped
   * by the container on the host.
   * <p>
   * The returned value is the address of the {@code dockerHost} unless the address is determined to
   * be localhost or 127.0.0.1.</p>
   */
  private String determineHeliosHost(final Info dockerInfo) throws HeliosDeploymentException {
    // note that checkDockerAndGetGateway is intentionally always called even if the return value
    // is discarded, as it does important checks about the local docker installation
    final String probeContainerGateway = checkDockerAndGetGateway();

    if (dockerHostAddressIsLocalhost()) {
      if (isDockerForMac(dockerInfo)) {
        try {
          log.info("determineHeliosHost: local environment appears to be Docker for Mac");
          return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
          throw new HeliosDeploymentException("Cannot resolve local hostname", e);
        }
      }

      log.info("determineHeliosHost: using value from probe container gateway: {}",
          probeContainerGateway);

      return probeContainerGateway;
    }

    log.info("determineHeliosHost: using docker host as helios host: {}", dockerHost.address());

    return dockerHost.address();
  }

  private boolean dockerHostAddressIsLocalhost() {
    return dockerHost.address().equals("localhost") || dockerHost.address().equals("127.0.0.1");
  }

  /** Returns the DockerHost that the container should use to refer to the docker daemon. */
  private DockerHost containerDockerHost(final Info dockerInfo) {
    if (isBoot2Docker(dockerInfo)) {
      return DockerHost.from(DockerHost.defaultUnixEndpoint(), null);
    }

    // otherwise use the normal DockerHost, *unless* DOCKER_HOST is set to
    // localhost or 127.0.0.1 - which will never work inside a container. For those cases, we
    // override the settings and use the unix socket instead.
    if (dockerHostAddressIsLocalhost()) {
      final String endpoint = DockerHost.defaultUnixEndpoint();
      log.warn("DOCKER_HOST points to localhost or 127.0.0.1. Replacing this with {} "
               + "as localhost/127.0.0.1 will not work inside a container to talk to the docker "
               + "daemon on the host itself.", endpoint);
      return DockerHost.from(endpoint, dockerHost.dockerCertPath());
    }

    return dockerHost;
  }

  @Override
  public HostAndPort address() {
    return deploymentAddress;
  }

  public String agentName() {
    return agentName;
  }

  private boolean isBoot2Docker(final Info dockerInfo) {
    return dockerInfo.operatingSystem().contains(BOOT2DOCKER_SIGNATURE);
  }

  private boolean isDockerForMac(final Info dockerInfo) {
    return "moby".equals(dockerInfo.name());
  }

  private List<String> buildContainerEnvironmentVariables(final Builder builder) {
    final Set<String> env = new HashSet<>(builder.env);
    env.add("DOCKER_HOST=" + containerDockerHost.bindUri().toString());

    if (!isNullOrEmpty(containerDockerHost.dockerCertPath())) {
      env.add("DOCKER_CERT_PATH=/certs");
    }

    // if gcr credentials were specified in the config, pass them in as an argument to helios-agent
    // note this only works inside the container if we also bind the volume containing the file
    for (File gcrCredentials : builder.googleContainerRegistryCredentials.asSet()) {
      final String arg = "--docker-gcp-account-credentials=" + gcrCredentials.getAbsolutePath();
      env.add("HELIOS_AGENT_OPTS=" + arg);

      log.info("set HELIOS_AGENT_OPTS inside the container to: {}", arg);
    }

    return ImmutableList.copyOf(env);
  }

  private List<String> buildContainerBinds(final Builder builder) {
    final Set<String> binds = new HashSet<>();

    if (containerDockerHost.bindUri().getScheme().equals("unix")) {
      final String path = containerDockerHost.bindUri().getPath();
      binds.add(path + ":" + path);
    }

    if (!isNullOrEmpty(containerDockerHost.dockerCertPath())) {
      binds.add(containerDockerHost.dockerCertPath() + ":/certs");
    }

    // see note in buildContainerEnvironmentVariables about these credentials
    for (File gcrCredentials : builder.googleContainerRegistryCredentials.asSet()) {
      final String directory = gcrCredentials.getAbsoluteFile().getParent();
      binds.add(directory + ":" + directory + ":ro");

      log.info("automatically adding volume bind for directory containing "
               + "Google Container Registry credentials at: {}", directory);
    }

    return ImmutableList.copyOf(binds);
  }

  /**
   * Checks that the local Docker daemon is reachable from inside a container.
   * This method also gets the gateway IP address for this HeliosSoloDeployment.
   *
   * @return The gateway IP address of the gateway probe container.
   *
   * @throws HeliosDeploymentException if we can't deploy the probe container or can't reach
   *                                   Docker daemon's API from inside the container.
   */
  private String checkDockerAndGetGateway() throws HeliosDeploymentException {
    log.info("checking that docker can be reached from within a container");

    final String probeName = randomString();
    final HostConfig hostConfig = HostConfig.builder()
        .binds(binds)
        .build();
    final ContainerConfig containerConfig = ContainerConfig.builder()
        .env(env)
        .hostConfig(hostConfig)
        .image(PROBE_IMAGE)
        .cmd(probeCommand(probeName))
        .build();

    final ContainerCreation creation;
    try {
      pullIfAbsent(PROBE_IMAGE);
      creation = dockerClient.createContainer(containerConfig, probeName);
    } catch (DockerException | InterruptedException e) {
      throw new HeliosDeploymentException("helios-solo probe container creation failed", e);
    }

    final ContainerExit exit;
    final String gateway;
    try {
      dockerClient.startContainer(creation.id());
      gateway = dockerClient.inspectContainer(creation.id())
          .networkSettings().gateway();
      exit = dockerClient.waitContainer(creation.id());
    } catch (DockerException | InterruptedException e) {
      killContainer(creation.id());
      throw new HeliosDeploymentException("helios-solo probe container failed", e);
    } finally {
      removeContainer(creation.id());
    }

    if (exit.statusCode() != 0) {
      throw new HeliosDeploymentException(String.format(
          "Docker was not reachable (curl exit status %d) using DOCKER_HOST=%s and "
          + "DOCKER_CERT_PATH=%s from within a container. Please ensure that "
          + "DOCKER_HOST contains a full hostname or IP address, not localhost, "
          + "127.0.0.1, etc.",
          exit.statusCode(),
          containerDockerHost.bindUri(),
          containerDockerHost.dockerCertPath()));
    }

    return gateway;
  }

  private void pullIfAbsent(final String image) throws DockerException, InterruptedException {
    try {
      dockerClient.inspectImage(image);
      log.info("image {} is present. Not pulling it.", image);
      return;
    } catch (ImageNotFoundException e) {
      log.info("pulling new image: {}", image);
    }
    dockerClient.pull(image);
  }

  private List<String> probeCommand(final String probeName) {
    final List<String> cmd = new ArrayList<>(ImmutableList.of("curl", "-f"));
    switch (containerDockerHost.uri().getScheme()) {
      case "unix":
        // A note on the URLs used below: since 7.50, curl requires a hostname when
        // using unix-sockets. See https://github.com/curl/curl/issues/936 and
        // https://github.com/docker/docker/pull/27640. The hostname we use does not matter since
        // curl is establishing a connection to the unix socket anyway.
        cmd.addAll(ImmutableList.of(
            "--unix-socket", containerDockerHost.uri().getSchemeSpecificPart(),
            "http://docker/containers/" + probeName + "/json"));
        break;
      case "https":
        cmd.addAll(ImmutableList.of(
            "--insecure",
            "--cert", "/certs/cert.pem",
            "--key", "/certs/key.pem",
            containerDockerHost.uri() + "/containers/" + probeName + "/json"));
        break;
      default:
        cmd.add(containerDockerHost.uri() + "/containers/" + probeName + "/json");
        break;
    }
    return ImmutableList.copyOf(cmd);
  }

  /**
   * @param heliosHost The address at which the Helios agent should expect to find the Helios
   *                   master.
   *
   * @return The container ID of the Helios Solo container.
   *
   * @throws HeliosDeploymentException if Helios Solo could not be deployed.
   */
  private String deploySolo(final String heliosHost) throws HeliosDeploymentException {
    final List<String> env = new ArrayList<>();
    env.addAll(this.env);
    env.add("HELIOS_NAME=" + agentName);
    env.add("HELIOS_ID=" + this.namespace + HELIOS_ID_SUFFIX);
    env.add("HOST_ADDRESS=" + heliosHost);

    final String heliosPort = String.format("%d/tcp", HELIOS_MASTER_PORT);
    final Map<String, List<PortBinding>> portBindings = ImmutableMap.of(
        heliosPort, singletonList(PortBinding.of("0.0.0.0", "")));
    final HostConfig hostConfig = HostConfig.builder()
        .portBindings(portBindings)
        .binds(binds)
        .build();
    final ContainerConfig containerConfig = ContainerConfig.builder()
        .env(ImmutableList.copyOf(env))
        .hostConfig(hostConfig)
        .image(heliosSoloImage)
        .build();

    log.info("starting container for helios-solo with containerConfig={}", containerConfig);

    final ContainerCreation creation;
    try {
      if (pullBeforeCreate) {
        dockerClient.pull(heliosSoloImage);
      }
      final String containerName = HELIOS_CONTAINER_PREFIX + this.namespace;
      creation = dockerClient.createContainer(containerConfig, containerName);
    } catch (DockerException | InterruptedException e) {
      throw new HeliosDeploymentException("helios-solo container creation failed", e);
    }

    try {
      dockerClient.startContainer(creation.id());
    } catch (DockerException | InterruptedException e) {
      killContainer(creation.id());
      removeContainer(creation.id());
      throw new HeliosDeploymentException("helios-solo container start failed", e);
    }

    log.info("helios-solo container started, containerId={}", creation.id());

    return creation.id();
  }


  private void killContainer(String id) {
    try {
      dockerClient.killContainer(id);
    } catch (DockerException | InterruptedException e) {
      log.warn("unable to kill container {}", id, e);
    }
  }

  private void removeContainer(String id) {
    try {
      dockerClient.removeContainer(id);
    } catch (DockerException | InterruptedException e) {
      log.warn("unable to remove container {}", id, e);
    }
  }

  /**
   * Return the first host port bound to the requested container port.
   *
   * @param containerId   The container in which to find the requested port.
   * @param containerPort The container port to resolve to a host port.
   *
   * @return The first host port bound to the requested container port.
   *
   * @throws HeliosDeploymentException when no host port is found.
   */
  private String getHostPort(final String containerId, final int containerPort)
      throws HeliosDeploymentException {
    final String heliosPort = String.format("%d/tcp", containerPort);
    try {
      final NetworkSettings settings = dockerClient.inspectContainer(containerId).networkSettings();
      for (final Map.Entry<String, List<PortBinding>> entry : settings.ports().entrySet()) {
        if (entry.getKey().equals(heliosPort)) {
          return entry.getValue().get(0).hostPort();
        }
      }
    } catch (DockerException | InterruptedException e) {
      throw new HeliosDeploymentException(String.format(
          "unable to find port binding for %s in container %s.",
          heliosPort,
          containerId),
          e);
    }
    throw new HeliosDeploymentException(String.format(
        "unable to find port binding for %s in container %s.",
        heliosPort,
        containerId));
  }

  private String randomString() {
    return Integer.toHexString(new Random().nextInt());
  }

  /**
   * @return A helios client connected to the master of this HeliosSoloDeployment.
   */
  public HeliosClient client() {
    return this.heliosClient;
  }

  /**
   * @return The container ID of the Helios Solo container.
   */
  public String heliosContainerId() {
    return heliosContainerId;
  }

  /**
   * Undeploy (shut down) this HeliosSoloDeployment.
   */
  public void close() {
    log.info("shutting ourselves down");

    undeployLeftoverJobs();

    killContainer(heliosContainerId);
    if (removeHeliosSoloContainerOnExit) {
      removeContainer(heliosContainerId);
      log.info("Stopped and removed HeliosSolo on host={} containerId={}",
          containerDockerHost, heliosContainerId);
    } else {
      log.info("Stopped (but did not remove) HeliosSolo on host={} containerId={}",
          containerDockerHost, heliosContainerId);
    }

    if (logService != null) {
      logService.stopAsync();
    }

    this.dockerClient.close();
  }

  /**
   * Undeploy jobs left over by {@link TemporaryJobs}. TemporaryJobs should clean these up,
   * but sometimes a few are left behind for whatever reason.
   */
  @VisibleForTesting
  protected void undeployLeftoverJobs() {
    try {
      // See if there are jobs running on any helios agent. If we are using TemporaryJobs,
      // that class should've undeployed them at this point.
      // Any jobs still running at this point have only been partially cleaned up.
      // We look for jobs via hostStatus() because the job may have been deleted from the master,
      // but the agent may still not have had enough time to undeploy the job from itself.
      final List<String> hosts = heliosClient.listHosts().get();
      for (final String host : hosts) {
        final HostStatus hostStatus = heliosClient.hostStatus(host).get();
        final Map<JobId, TaskStatus> statuses = hostStatus.getStatuses();

        for (final Map.Entry<JobId, TaskStatus> status : statuses.entrySet()) {
          final JobId jobId = status.getKey();
          final Goal goal = status.getValue().getGoal();
          if (goal != Goal.UNDEPLOY) {
            log.info("Job {} is still set to {} on host {}. Undeploying it now.",
                jobId, goal, host);
            final JobUndeployResponse undeployResponse = heliosClient.undeploy(jobId, host).get();
            log.info("Undeploy response for job {} is {}.", jobId, undeployResponse.getStatus());

            if (undeployResponse.getStatus() != JobUndeployResponse.Status.OK) {
              log.warn("Undeploy response for job {} was not OK. This could mean that something "
                       + "beat the helios-solo master in telling the helios-solo agent to "
                       + "undeploy.", jobId);
            }
          }

          log.info("Waiting for job {} to actually be undeployed...", jobId);
          awaitJobUndeployed(heliosClient, host, jobId, jobUndeployWaitSeconds, TimeUnit.SECONDS);
          log.info("Job {} successfully undeployed.", jobId);
        }
      }
    } catch (Exception e) {
      log.warn("Exception occurred when trying to clean up leftover jobs.", e);
    }
  }

  private Boolean awaitJobUndeployed(final HeliosClient client, final String host,
                                     final JobId jobId, final int timeout,
                                     final TimeUnit timeunit) throws Exception {
    return Polling.await(timeout, timeunit, "Job " + jobId + " did not undeploy after %d %s",
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            final HostStatus hostStatus = getOrNull(client.hostStatus(host));
            if (hostStatus == null) {
              log.debug("Job {} host status is null. Waiting...", jobId);
              return null;
            }
            final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
            if (taskStatus != null) {
              log.debug("Job {} task status is {}.", jobId, taskStatus.getState());
              return null;
            }
            log.info("Task status is null which means job {} has been successfully undeployed.",
                jobId);
            return true;
          }
        });
  }

  private <T> T getOrNull(final ListenableFuture<T> future)
      throws ExecutionException, InterruptedException {
    return Futures.catching(future, Exception.class, new Function<Exception, T>() {
      @Override
      public T apply(final Exception ex) {
        return null;
      }
    }).get();
  }

  /**
   * @return A Builder that can be used to instantiate a HeliosSoloDeployment.
   */
  public static Builder builder() {
    return builder(null);
  }

  /**
   * @param profile A configuration profile used to populate builder options.
   *
   * @return A Builder that can be used to instantiate a HeliosSoloDeployment.
   */
  public static Builder builder(final String profile) {
    return new Builder(profile, HeliosConfig.loadConfig());
  }

  /**
   * @return a Builder with its Docker Client configured automatically using the
   *         <code>DOCKER_HOST</code> and <code>DOCKER_CERT_PATH</code> environment variables, or
   *         sensible defaults if they are absent.
   */
  public static Builder fromEnv() {
    return fromEnv(null);
  }

  /**
   * @param profile A configuration profile used to populate builder options.
   *
   * @return a Builder with its Docker Client configured automatically using the
   *         <code>DOCKER_HOST</code> and <code>DOCKER_CERT_PATH</code> environment variables, or
   *         sensible defaults if they are absent.
   */
  public static Builder fromEnv(final String profile) {
    try {
      return builder(profile).dockerClient(DefaultDockerClient.fromEnv().build());
    } catch (DockerCertificateException ex) {
      throw new RuntimeException("unable to create Docker client from environment", ex);
    }
  }

  @Override
  public String toString() {
    return "HeliosSoloDeployment{"
           + "deploymentAddress=" + deploymentAddress
           + ", dockerHost=" + dockerHost
           + ", heliosContainerId=" + heliosContainerId
           + '}';
  }

  public static class Builder {
    private DockerClient dockerClient;
    private DockerHost dockerHost;
    private DockerHost containerDockerHost;
    private HeliosClient heliosClient;
    private String heliosSoloImage = "spotify/helios-solo:latest";
    private String namespace;
    private String heliosUsername;
    private Set<String> env;
    private boolean pullBeforeCreate = true;
    private boolean removeHeliosSoloContainerOnExit = false;
    private int jobUndeployWaitSeconds = DEFAULT_WAIT_SECONDS;

    // Intentionally picking a publicly accessible class for this log output
    private LogStreamFollower logStreamFollower =
        LoggingLogStreamFollower.create(LoggerFactory.getLogger(TemporaryJob.class));

    private Optional<File> googleContainerRegistryCredentials = Optional.absent();

    Builder(String profile, Config rootConfig) {
      this.env = new HashSet<>();

      final Config config;
      if (profile == null) {
        config = HeliosConfig.getDefaultProfile(
            HELIOS_SOLO_PROFILE, HELIOS_SOLO_PROFILES, rootConfig);
      } else {
        config = HeliosConfig.getProfile(HELIOS_SOLO_PROFILES, profile, rootConfig);
      }

      if (config.hasPath("image")) {
        heliosSoloImage(config.getString("image"));
      }
      if (config.hasPath("namespace")) {
        namespace(config.getString("namespace"));
      }
      if (config.hasPath("username")) {
        namespace(config.getString("username"));
      }
      if (config.hasPath("env")) {
        for (final Map.Entry<String, ConfigValue> entry : config.getConfig("env").entrySet()) {
          env(entry.getKey(), entry.getValue().unwrapped());
        }
      }

      if (config.hasPath("google-container-registry")) {
        configureGoogleContainerRegistry(config.getConfig("google-container-registry"));
      }
    }

    private void configureGoogleContainerRegistry(Config subConfig) {
      if (subConfig.hasPath("credentials")) {
        final String path = subConfig.getString("credentials");
        final File credentialsFile = new File(path);

        if (credentialsFile.exists()) {
          this.googleContainerRegistryCredentials = Optional.of(credentialsFile);
        } else {
          log.warn("Ignoring non-existent google-container-registry.credentials file: {}", path);
        }
      }
    }

    /**
     * By default, the {@link #heliosSoloImage} will be checked for updates before creating a
     * container by doing a "docker pull". Call this method with "false" to disable this behavior.
     */
    public Builder checkForNewImages(boolean enabled) {
      this.pullBeforeCreate = enabled;
      return this;
    }

    /**
     * By default the container running helios-solo is removed when
     * {@link HeliosSoloDeployment#close()} is called. Call this method with "false" to disable this
     * (which is probably only useful for developing helios-solo or this class itself and
     * inspecting logs).
     */
    public Builder removeHeliosSoloOnExit(boolean enabled) {
      this.removeHeliosSoloContainerOnExit = enabled;
      return this;
    }

    /**
     * Set the number of seconds Helios solo will wait for jobs to be undeployed and, as a result,
     * their associated Docker containers to stop running before shutting itself down.
     * The default is 30 seconds.
     */
    public Builder jobUndeployWaitSeconds(int seconds) {
      this.jobUndeployWaitSeconds = seconds;
      return this;
    }

    /**
     * Specify a Docker client to be used for this Helios Solo deployment. A Docker client is
     * necessary in order to deploy Helios Solo.
     *
     * @param dockerClient A client connected to the Docker instance in which to deploy Helios Solo.
     *
     * @return This Builder, with its Docker client configured.
     */
    public Builder dockerClient(final DockerClient dockerClient) {
      this.dockerClient = dockerClient;
      return this;
    }

    /**
     * Optionally specify a DockerHost (i.e. Docker socket and certificate info) to connect to
     * Docker from the host OS. If unset the <code>DOCKER_HOST</code> and
     * <code>DOCKER_CERT_PATH</code> environment variables will be used. If said variables are not
     * present sensible defaults will be used.
     *
     * @param dockerHost Docker socket and certificate settings for the host OS.
     *
     * @return This Builder, with its Docker host configured.
     */
    public Builder dockerHost(final DockerHost dockerHost) {
      this.dockerHost = dockerHost;
      return this;
    }

    /**
     * Optionally specify a DockerHost (i.e. Docker socket and certificate info) to connect to
     * Docker from inside the Helios container. If unset sensible defaults will be derived from
     * the <code>DOCKER_HOST</code> and <code>DOCKER_CERT_PATH</code> environment variables and the
     * Docker daemon's configuration.
     *
     * @param containerDockerHost Docker socket and certificate settings as seen from inside
     *                            the Helios container.
     *
     * @return This Builder, with its container Docker host configured.
     */
    public Builder containerDockerHost(final DockerHost containerDockerHost) {
      this.containerDockerHost = containerDockerHost;
      return this;
    }

    /**
     * Optionally specify a {@link HeliosClient}. Used for unit tests.
     *
     * @param heliosClient HeliosClient
     *
     * @return This Builder, with its HeliosClient configured.
     */
    public Builder heliosClient(final HeliosClient heliosClient) {
      this.heliosClient = heliosClient;
      return this;
    }

    /**
     * Customize the image used for helios-solo. If not set defaults to
     * "spotify/helios-solo:latest".
     */
    public Builder heliosSoloImage(String image) {
      this.heliosSoloImage = Preconditions.checkNotNull(image);
      return this;
    }

    /**
     * Optionally specify a unique namespace for the Helios solo agent and Docker container names.
     * If unset a random string will be used.
     *
     * @param namespace A unique namespace for the Helios solo agent and Docker container.
     *
     * @return This Builder, with its namespace configured.
     */
    public Builder namespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * Optionally specify the username to be used by the {@link HeliosClient} connected to the
     * {@link HeliosSoloDeployment} created with this Builder. If unset a random string will be
     * used.
     *
     * @param username The Helios user to identify as.
     *
     * @return This Builder, with its Helios username configured.
     */
    public Builder heliosUsername(final String username) {
      this.heliosUsername = username;
      return this;
    }

    /**
     * Optionally provide a custom {@link LogStreamFollower} that provides streams for writing
     * container stdout/stderr logs. If set to null, logging of container stdout/stderr will be
     * disabled.
     *
     * @param logStreamFollower The provider to use.
     *
     * @return This Builder, with its log stream provider configured.
     */
    public Builder logStreamProvider(final LogStreamFollower logStreamFollower) {
      this.logStreamFollower = logStreamFollower;
      return this;
    }

    /**
     * Optionally specify an environment variable to be set inside the Helios solo container.
     *
     * @param key   Environment variable to set.
     * @param value Environment variable value.
     *
     * @return This Builder, with the environment variable configured.
     */
    public Builder env(final String key, final Object value) {
      this.env.add(key + "=" + value.toString());
      return this;
    }

    /**
     * Override the credentials used for Google Container Registry. If not set, then the builder
     * will default to checking to see if an entry for "googleContainerRegistryCredentials" exists
     * in the configuration profile.
     */
    public Builder googleContainerRegistryCredentials(File credentials) {
      this.googleContainerRegistryCredentials = Optional.of(credentials);
      return this;
    }

    /**
     * Configures, deploys, and returns a {@link HeliosSoloDeployment} using the as specified by
     * this Builder.
     *
     * @return A Helios Solo deployment configured by this Builder.
     */
    public HeliosSoloDeployment build() {
      this.env = ImmutableSet.copyOf(this.env);
      return new HeliosSoloDeployment(this);
    }
  }
}

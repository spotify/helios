/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.testing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.DockerHost;
import com.spotify.docker.client.ImageNotFoundException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Info;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Collections.singletonList;

/**
 * A HeliosSoloDeployment represents a deployment of Helios Solo, which is to say one Helios
 * master and one Helios agent deployed in Docker. Helios Solo uses the Docker instance it is
 * deployed on to run its jobs.
 */
public class HeliosSoloDeployment implements HeliosDeployment {

  private static final Logger log = LoggerFactory.getLogger(HeliosSoloDeployment.class);

  private static final String BOOT2DOCKER_SIGNATURE = "Boot2Docker";
  static final String PROBE_IMAGE = "onescience/alpine:latest";
  private static final String HELIOS_NAME_SUFFIX = ".solo.local"; //  Required for SkyDNS discovery.
  private static final String HELIOS_ID_SUFFIX = "-solo-host";
  private static final String HELIOS_CONTAINER_PREFIX = "helios-solo-container-";
  private static final String HELIOS_SOLO_PROFILE = "helios.solo.profile";
  private static final String HELIOS_SOLO_PROFILES = "helios.solo.profiles.";
  static final int HELIOS_MASTER_PORT = 5801;
  static final int HELIOS_SOLO_WATCHDOG_PORT = 33333;
  private static final int DEFAULT_WAIT_SECONDS = 30;

  private final DockerClient dockerClient;
  /** The DockerHost we use to communicate with docker */
  private final DockerHost dockerHost;
  /** The DockerHost the container uses to communicate with docker */
  private final DockerHost containerDockerHost;
  private final String heliosSoloImage;
  private final boolean pullBeforeCreate;
  private final String namespace;
  private final List<String> env;
  private final List<String> binds;
  private final String heliosContainerId;
  private final HostAndPort deploymentAddress;
  private final HeliosClient heliosClient;
  private boolean removeHeliosSoloContainerOnExit;
  private final int jobUndeployWaitSeconds;
  private final SoloMasterProber soloMasterProber;
  private final SoloHostProber soloHostProber;
  private final Socket watchdogSocket;

  HeliosSoloDeployment(final Builder builder) {
    this.heliosSoloImage = builder.heliosSoloImage;
    this.pullBeforeCreate = builder.pullBeforeCreate;
    this.removeHeliosSoloContainerOnExit = builder.removeHeliosSoloContainerOnExit;
    this.jobUndeployWaitSeconds = builder.jobUndeployWaitSeconds;
    this.soloMasterProber = checkNotNull(builder.soloMasterProber, "soloMasterProber");
    this.soloHostProber = checkNotNull(builder.soloHostProber, "soloHostProber");

    final String username = Optional.fromNullable(builder.heliosUsername).or(randomString());

    this.dockerClient = checkNotNull(builder.dockerClient, "dockerClient");
    this.dockerHost = Optional.fromNullable(builder.dockerHost).or(DockerHost.fromEnv());
    this.containerDockerHost = Optional.fromNullable(builder.containerDockerHost)
        .or(containerDockerHost());
    this.namespace = Optional.fromNullable(builder.namespace).or(randomString());
    this.env = containerEnv(builder.env);
    this.binds = containerBinds();

    final String heliosHost;
    final String heliosPort;
    final int watchdogPort;
    //TODO(negz): Determine and propagate NetworkManager DNS servers?
    try {
      log.info("checking that docker can be reached from within a container");
      final String probeContainerGateway = checkDockerAndGetGateway();
      if (dockerHost.address().equals("localhost") || dockerHost.address().equals("127.0.0.1")) {
        heliosHost = probeContainerGateway;
      } else {
        heliosHost = dockerHost.address();
      }
      this.heliosContainerId = deploySolo(heliosHost);
      heliosPort = getHostPort(this.heliosContainerId, HELIOS_MASTER_PORT);
      watchdogPort = Integer.parseInt(getHostPort(
          this.heliosContainerId, HELIOS_SOLO_WATCHDOG_PORT));
    } catch (HeliosDeploymentException e) {
      log.error("Unable to deploy helios-solo container: {}", e);
      throw new AssertionError("Unable to deploy helios-solo container.", e);
    }

    // Running the String host:port through HostAndPort does some validation for us.
    this.deploymentAddress = HostAndPort.fromString(dockerHost.address() + ":" + heliosPort);
    this.heliosClient = Optional.fromNullable(builder.heliosClient).or(
        HeliosClient.newBuilder()
            .setUser(username)
            .setEndpoints("http://" + deploymentAddress)
            .build());

    watchdogSocket = SoloWatchdogConnector.connectWithTimeout(
        dockerHost.address(), watchdogPort, 60, TimeUnit.SECONDS);

    try {
      start();
    } catch (TimeoutException e) {
      Throwables.propagate(e);
    }
  }

  private void start() throws TimeoutException {
    // wait for the helios master to be available
    Polling.awaitUnchecked(30, TimeUnit.SECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return soloMasterProber.check(deploymentAddress);
      }
    });

    // Ensure that at least one agent is available and UP in this HeliosDeployment.
    // This prevents continuing with the test when starting up helios-solo before the agent is
    // registered.
    Polling.awaitUnchecked(30, TimeUnit.SECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return soloHostProber.check(heliosClient, deploymentAddress);
      }
    });
  }

  /** Returns the DockerHost that the container should use to refer to the docker daemon. */
  private DockerHost containerDockerHost() {
    if (isBoot2Docker(dockerInfo())) {
      return DockerHost.from(DefaultDockerClient.DEFAULT_UNIX_ENDPOINT, null);
    }

    // otherwise use the normal DockerHost, *unless* DOCKER_HOST is set to
    // localhost or 127.0.0.1 - which will never work inside a container. For those cases, we
    // override the settings and use the unix socket instead.
    if (this.dockerHost.address().equals("localhost") ||
        this.dockerHost.address().equals("127.0.0.1")) {
      final String endpoint = DockerHost.DEFAULT_UNIX_ENDPOINT;
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

  private boolean isBoot2Docker(final Info dockerInfo) {
    return dockerInfo.operatingSystem().contains(BOOT2DOCKER_SIGNATURE);
  }

  private Info dockerInfo() {
    try {
      return this.dockerClient.info();
    } catch (DockerException | InterruptedException e) {
      // There's not a lot we can do if Docker is unreachable.
      throw Throwables.propagate(e);
    }
  }

  private List<String> containerEnv(final Set builderEnv) {
    final HashSet<String> env = new HashSet<>();
    env.addAll(builderEnv);
    env.add("DOCKER_HOST=" + containerDockerHost.bindURI().toString());
    if (!isNullOrEmpty(containerDockerHost.dockerCertPath())) {
      env.add("DOCKER_CERT_PATH=/certs");
    }

    // Inform helios-solo  that it's being used for helios-testing and that it should kill itself
    // when no longer used.
    env.add("HELIOS_SOLO_SUICIDE=1");

    return ImmutableList.copyOf(env);
  }

  private List<String> containerBinds() {
    final HashSet<String> binds = new HashSet<>();
    if (containerDockerHost.bindURI().getScheme().equals("unix")) {
      final String path = containerDockerHost.bindURI().getPath();
      binds.add(path + ":" + path);
    }
    if (!isNullOrEmpty(containerDockerHost.dockerCertPath())) {
      binds.add(containerDockerHost.dockerCertPath() + ":/certs");
    }
    return ImmutableList.copyOf(binds);
  }

  /**
   * Checks that the local Docker daemon is reachable from inside a container.
   * This method also gets the gateway IP address for this HeliosSoloDeployment.
   *
   * @return The gateway IP address of the gateway probe container.
   * @throws HeliosDeploymentException if we can't deploy the probe container or can't reach the
   * Docker daemon's API from inside the container.
   */
  private String checkDockerAndGetGateway() throws HeliosDeploymentException {
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
      gateway = dockerClient.inspectContainer(creation.id()).networkSettings().gateway();
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
          containerDockerHost.bindURI(),
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
        cmd.addAll(ImmutableList.of(
            "--unix-socket", containerDockerHost.uri().getSchemeSpecificPart(),
            "http:/containers/" + probeName + "/json"));
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
   * @return The container ID of the Helios Solo container.
   * @throws HeliosDeploymentException if Helios Solo could not be deployed.
   */
  private String deploySolo(final String heliosHost) throws HeliosDeploymentException {
    //TODO(negz): Don't make this.env immutable so early?
    final List<String> env = new ArrayList<>();
    env.addAll(this.env);
    env.add("HELIOS_NAME=" + this.namespace + HELIOS_NAME_SUFFIX);
    env.add("HELIOS_ID=" + this.namespace + HELIOS_ID_SUFFIX);
    env.add("HOST_ADDRESS=" + heliosHost);

    final String heliosPort = String.format("%d/tcp", HELIOS_MASTER_PORT);
    final String watchdogPort = String.format("%d/tcp", HELIOS_SOLO_WATCHDOG_PORT);
    final Map<String, List<PortBinding>> portBindings = ImmutableMap.of(
        heliosPort, singletonList(PortBinding.randomPort("0.0.0.0")),
        watchdogPort, singletonList(PortBinding.randomPort("0.0.0.0"))
    );
    final HostConfig hostConfig = HostConfig.builder()
        .portBindings(portBindings)
        .binds(binds)
        .build();
    final ContainerConfig containerConfig = ContainerConfig.builder()
        .env(ImmutableList.copyOf(env))
        .hostConfig(hostConfig)
        .image(heliosSoloImage)
        .build();

    log.info("starting container for helios-solo with image={}", heliosSoloImage);

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


  private void killContainer(final String id) {
    try {
      dockerClient.killContainer(id);
    } catch (DockerException | InterruptedException e) {
      log.warn("unable to kill container {}", id, e);
    }
  }

  private void removeContainer(final String id) {
    try {
      dockerClient.removeContainer(id);
    } catch (DockerException | InterruptedException e) {
      log.warn("unable to remove container {}", id, e);
    }
  }

  /**
   * Return the first host port bound to the requested container port.
   *
   * @param containerId The container in which to find the requested port.
   * @param containerPort The container port to resolve to a host port.
   * @return The first host port bound to the requested container port.
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
          "unable to find port binding for %s in container %s.", heliosPort, containerId), e);
    }
    throw new HeliosDeploymentException(String.format(
        "unable to find port binding for %s in container %s.", heliosPort, containerId));
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

    try {
      watchdogSocket.close();
    } catch (IOException ignored) {
    }

    final TimeLimiter timeLimiter = new SimpleTimeLimiter(Executors.newSingleThreadExecutor());
    try {
      timeLimiter.callWithTimeout(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          dockerClient.waitContainer(heliosContainerId);
          return true;
        }
      }, 60, TimeUnit.SECONDS, false);
    } catch (Exception e) {
      log.warn("Timed out waiting for helios-solo container {} to exit. Forcibly killing it",
               heliosContainerId, e);
      killContainer(heliosContainerId);
    }

    if (removeHeliosSoloContainerOnExit) {
      removeContainer(heliosContainerId);
      log.info("Stopped and removed HeliosSolo on host={} containerId={}",
               containerDockerHost, heliosContainerId);
    } else {
      log.info("Stopped (but did not remove) HeliosSolo on host={} containerId={}",
               containerDockerHost, heliosContainerId);
    }

    this.dockerClient.close();
  }

  private Boolean awaitJobUndeployed(final HeliosClient client, final String host,
                                     final JobId jobId, final int timeout,
                                     final TimeUnit timeunit) throws Exception {
    return Polling.await(timeout, timeunit, new Callable<Boolean>() {
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
        log.info("Task status is null which means job {} has been successfully undeployed.", jobId);
        return true;
      }
    });
  }

  private <T> T getOrNull(final ListenableFuture<T> future)
      throws ExecutionException, InterruptedException {
    return Futures.withFallback(future, new FutureFallback<T>() {
      @Override
      public ListenableFuture<T> create(@NotNull final Throwable t) throws Exception {
        return Futures.immediateFuture(null);
      }
    }).get();
  }

  /**
   * @return A Builder that can be used to instantiate a HeliosSoloDeployment.
   */
  public static Builder builder() {
    return builderWithProfile(null);
  }

  /**
   * @param profile A configuration profile used to populate builder options.
   * @return A Builder that can be used to instantiate a HeliosSoloDeployment.
   */
  public static Builder builderWithProfile(final String profile) {
    return builderWithProfileAndConfig(profile, HeliosConfig.loadConfig("helios-solo"));
  }

  @VisibleForTesting
  static Builder builderWithProfileAndConfig(final String profile, final Config config) {
    return new Builder(profile, config);
  }

  /**
   * @return A Builder with its Docker Client configured automatically using the
   * <code>DOCKER_HOST</code> and <code>DOCKER_CERT_PATH</code> environment variables, or sensible
   * defaults if they are absent.
   */
  public static Builder fromEnv() {
    return fromEnv(null);
  }

  /**
   * @param profile A configuration profile used to populate builder options.
   * @return A Builder with its Docker Client configured automatically using the
   * <code>DOCKER_HOST</code> and <code>DOCKER_CERT_PATH</code> environment variables, or sensible
   * defaults if they are absent.
   */
  public static Builder fromEnv(final String profile)  {
    try {
      return builderWithProfile(profile).dockerClient(DefaultDockerClient.fromEnv().build());
    } catch (DockerCertificateException e) {
      throw new RuntimeException("unable to create Docker client from environment", e);
    }
  }

  @Override
  public String toString() {
    return "HeliosSoloDeployment{" +
           "dockerClient=" + dockerClient +
           ", dockerHost=" + dockerHost +
           ", containerDockerHost=" + containerDockerHost +
           ", heliosSoloImage='" + heliosSoloImage + '\'' +
           ", pullBeforeCreate=" + pullBeforeCreate +
           ", namespace='" + namespace + '\'' +
           ", env=" + env +
           ", binds=" + binds +
           ", heliosContainerId='" + heliosContainerId + '\'' +
           ", deploymentAddress=" + deploymentAddress +
           ", heliosClient=" + heliosClient +
           ", removeHeliosSoloContainerOnExit=" + removeHeliosSoloContainerOnExit +
           ", jobUndeployWaitSeconds=" + jobUndeployWaitSeconds +
           '}';
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
    private boolean removeHeliosSoloContainerOnExit = true;
    private int jobUndeployWaitSeconds = DEFAULT_WAIT_SECONDS;
    private SoloMasterProber soloMasterProber = new DefaultSoloMasterProber();
    private SoloHostProber soloHostProber = new DefaultSoloHostProber();

    Builder(final String profile, final Config rootConfig) {
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
     * @return This Builder, with its Helios username configured.
     */
    public Builder heliosUsername(final String username) {
      this.heliosUsername = username;
      return this;
    }

    /**
     * This method exists only for testing. We need to mock the logic that probes the master port.
     * @param prober A {@link SoloMasterProber}
     * @return This Builder, with its {@link SoloMasterProber} configured.
     */
    @VisibleForTesting
    Builder soloMasterProber(final SoloMasterProber prober) {
      this.soloMasterProber = prober;
      return this;
    }

    /**
     * This method exists only for testing. We need to mock the logic checking the solo agent is up.
     * @param prober A {@link SoloHostProber}
     * @return This Builder, with its {@link SoloHostProber} configured.
     */
    @VisibleForTesting
    Builder soloHostProber(final SoloHostProber prober) {
      this.soloHostProber = prober;
      return this;
    }

    /**
     * Optionally specify an environment variable to be set inside the Helios solo container.
     * @param key Environment variable to set.
     * @param value Environment variable value.
     * @return This Builder, with the environment variable configured.
     */
    public Builder env(final String key, final Object value) {
      this.env.add(key + "=" + value.toString());
      return this;
    }

    /**
     * Configures, deploys, and returns a {@link HeliosSoloDeployment} using the as specified by
     * this Builder.
     *
     * @return A Helios Solo deployment configured by this Builder.
     */
    public HeliosDeployment build() {
      this.env = ImmutableSet.copyOf(this.env);
      return new HeliosSoloDeployment(this);
    }
  }

  private static class DefaultSoloMasterProber implements SoloMasterProber {

    @Override
    public Boolean check(final HostAndPort hostAndPort) throws Exception {
      final SocketAddress address = new InetSocketAddress(hostAndPort.getHostText(),
                                                          hostAndPort.getPort());
      log.debug("attempting to connect to {}", address);

      try {
        final Socket s = new Socket();
        s.connect(address, 100);
        log.info("successfully connected to address {} for host and port {}", address, hostAndPort);
        return true;
      } catch (SocketTimeoutException | ConnectException e) {
        log.debug("could not connect to host and port {}: {}", hostAndPort, e.toString());
        return null;
      }
    }
  }

  private static class DefaultSoloHostProber implements SoloHostProber {

    @Override
    public Boolean check(final HeliosClient client, final HostAndPort hostAndPort)
        throws Exception {
      final ListenableFuture<List<String>> future = client.listHosts();

      final List<String> hosts;
      try {
        // use a short timeout to allow this request to be retried a few times by the
        // Polling.await loop
        hosts = future.get(1, TimeUnit.SECONDS);
      } catch (TimeoutException | InterruptedException e) {
        log.debug("timed out waiting for listHosts request to finish, will retry");
        return null;
      }

      if (hosts.isEmpty()) {
        log.debug("0 agents in {}, will retry", hostAndPort);
        return null;
      }

      // Check that at least one host is UP (is maintaining a reasonably reliable
      // connection to ZK) in addition to registering.
      final ListenableFuture<Map<String, HostStatus>> statusFuture = client.hostStatuses(hosts);
      final Map<String, HostStatus> hostStatuses;
      try {
        hostStatuses = statusFuture.get(1, TimeUnit.SECONDS);
      } catch (TimeoutException | InterruptedException e) {
        log.debug("timed out waiting for hostStatuses to finish, will retry");
        return null;
      }

      for (final HostStatus hostStatus : hostStatuses.values()) {
        if (hostStatus != null && hostStatus.getStatus() == HostStatus.Status.UP) {
          log.info("Ensured that at least one agent is UP in this HeliosDeployment, "
                   + "continuing with test!");
          return true;
        }
      }

      return null;
    }
  }
}

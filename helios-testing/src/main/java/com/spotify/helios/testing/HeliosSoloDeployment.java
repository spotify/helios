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

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Info;
import com.spotify.docker.client.messages.NetworkSettings;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.client.HeliosClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

  public static final String BOOT2DOCKER_SIGNATURE = "Boot2Docker";
  public static final String PROBE_IMAGE = "onescience/alpine:latest";
  public static final String HELIOS_IMAGE = "spotify/helios-solo:latest";
  public static final String HELIOS_NAME_PREFIX = "solo.local.";
  public static final String HELIOS_CONTAINER_PREFIX = "helios-solo-container-";
  public static final int HELIOS_MASTER_PORT = 5801;

  private final DockerClient dockerClient;
  private final DockerHost dockerHost;
  private final DockerHost containerDockerHost;
  private final String namespace;
  private final List<String> env;
  private final List<String> binds;
  private final String heliosContainerId;
  private final HostAndPort deploymentAddress;
  private final HeliosClient heliosClient;

  HeliosSoloDeployment(final Builder builder) {
    final String username = Optional.fromNullable(builder.heliosUsername).or(randomString());

    this.dockerClient = checkNotNull(builder.dockerClient, "dockerClient");
    this.dockerHost = Optional.fromNullable(builder.dockerHost).or(DockerHost.fromEnv());
    this.containerDockerHost = containerDockerHost(builder);
    this.namespace = Optional.fromNullable(builder.namespace).or(randomString());
    this.env = containerEnv();
    this.binds = containerBinds();

    final String heliosHost;
    final String heliosPort;
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
    } catch (HeliosDeploymentException e) {
      throw new AssertionError("Unable to deploy helios-solo container.", e);
    }

    // Running the String host:port through HostAndPort does some validation for us.
    this.deploymentAddress = HostAndPort.fromString(dockerHost.address() + ":" + heliosPort);
    this.heliosClient = HeliosClient.newBuilder()
            .setUser(username)
            .setEndpoints("http://" + deploymentAddress)
            .build();
  }

  private DockerHost containerDockerHost(final Builder builder) {
    if (builder.containerDockerHost != null) {
      return containerDockerHost;
    }

    if (isBoot2Docker(dockerInfo())) {
      return DockerHost.from(DefaultDockerClient.DEFAULT_UNIX_ENDPOINT, null);
    }

    // otherwise construct a DockerHost from environment variables, *unless* DOCKER_HOST is set to
    // localhost or 127.0.0.1 - which will never work inside a container. For those cases, we
    // override the settings and use the unix socket instead.
    final DockerHost dockerHost = DockerHost.fromEnv();
    if (dockerHost.address().equals("localhost") || dockerHost.address().equals("127.0.0.1")) {
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

  private List<String> containerEnv() {
    final HashSet<String> env = new HashSet<>();
    env.add("DOCKER_HOST=" + containerDockerHost.bindURI().toString());
    if (!isNullOrEmpty(containerDockerHost.dockerCertPath())) {
      env.add("DOCKER_CERT_PATH=/certs");
    }
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
      dockerClient.pull(PROBE_IMAGE);
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
              containerDockerHost.bindURI(),
              containerDockerHost.dockerCertPath()));
    }

    return gateway;
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
    env.add("HELIOS_NAME=" + HELIOS_NAME_PREFIX + this.namespace);
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
            .image(HELIOS_IMAGE)
            .build();

    log.info("starting container for helios-solo with image={}", HELIOS_IMAGE);

    final ContainerCreation creation;
    try {
      dockerClient.pull(HELIOS_IMAGE);
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
      for (Map.Entry<String, List<PortBinding>> entry : settings.ports().entrySet()) {
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
   * Undeploy (shut down) this HeliosSoloDeployment.
   */
  public void close() {
    log.info("shutting ourselves down");

    killContainer(heliosContainerId);
    removeContainer(heliosContainerId);
    log.info("Stopped and removed HeliosSolo on host={} containerId={}",
        containerDockerHost, heliosContainerId);
    this.dockerClient.close();
  }

  /**
   * @return A Builder that can be used to instantiate a HeliosSoloDeployment.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * @return A Builder with its Docker Client configured automatically using the
   * <code>DOCKER_HOST</code> and <code>DOCKER_CERT_PATH</code> environment variables, or sensible
   * defaults if they are absent.
   */
  public static Builder fromEnv()  {
    try {
      return builder().dockerClient(DefaultDockerClient.fromEnv().build());
    } catch (DockerCertificateException e) {
      throw new RuntimeException("unable to create Docker client from environment", e);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("deploymentAddress", deploymentAddress)
        .add("dockerHost", dockerHost)
        .add("heliosContainerId", heliosContainerId)
        .toString();
  }

  public static class Builder {
    private DockerClient dockerClient;
    private DockerHost dockerHost;
    private DockerHost containerDockerHost;
    private String namespace;
    private String heliosUsername;

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
     * @param dockerHost Docker socket and certificate settings for the Helios container.
     * @return This Builder, with its container Docker host configured.
     */
    public Builder containerDockerHost(final DockerHost dockerHost) {
      this.containerDockerHost = containerDockerHost;
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
     * Configures, deploys, and returns a {@link HeliosSoloDeployment} using the as specified by
     * this Builder.
     *
     * @return A Helios Solo deployment configured by this Builder.
     */
    public HeliosDeployment build() {
      return new HeliosSoloDeployment(this);
    }
  }
}

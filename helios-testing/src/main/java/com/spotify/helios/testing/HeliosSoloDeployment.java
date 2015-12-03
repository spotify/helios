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

import com.google.common.base.Optional;
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

public class HeliosSoloDeployment implements HeliosDeployment {

  private static final Logger log = LoggerFactory.getLogger(HeliosSoloDeployment.class);

  public static final String BOOT2DOCKER_SIGNATURE = "Boot2Docker";
  public static final String PROBE_IMAGE = "onescience/alpine:latest";
  public static final String HELIOS_IMAGE = "spotify/helios-solo:latest";
  public static final String HELIOS_NAME_PREFIX = "solo.local.";
  public static final String HELIOS_CONTAINER_PREFIX = "helios-solo-container-";
  public static final int HELIOS_MASTER_PORT = 5801;

  final DockerClient dockerClient;
  final DockerHost dockerHost;
  final DockerHost containerDockerHost;
  final String namespace;
  final List<String> env;
  final List<String> binds;
  final String heliosContainerId;
  final HeliosClient heliosClient;

  HeliosSoloDeployment(final Builder builder) {
    final String username = Optional.fromNullable(builder.heliosUsername).or(randomString());

    this.dockerClient = checkNotNull(builder.dockerClient, "dockerClient");
    this.dockerHost = Optional.fromNullable(builder.dockerHost).or(DockerHost.fromEnv());
    this.containerDockerHost = Optional.fromNullable(builder.containerDockerHost)
            .or(containerDockerHostFromEnv());
    this.namespace = Optional.fromNullable(builder.namespace).or(randomString());
    this.env = containerEnv();
    this.binds = containerBinds();

    final String heliosHost;
    final String heliosPort;
    //TODO(negz): Determine and propagate NetworkManager DNS servers?
    try {
      assertDockerReachableFromContainer();
      if (dockerHost.address().equals("localhost") || dockerHost.address().equals("127.0.0.1")) {
          heliosHost = containerGateway();
      } else {
        heliosHost = dockerHost.address();
      }
      this.heliosContainerId = deploySolo(heliosHost);
      heliosPort = getHeliosMasterPort(this.heliosContainerId, HELIOS_MASTER_PORT);
    } catch (HeliosDeploymentException e) {
      throw new AssertionError("Unable to deploy helios-solo container.", e);
    }

    // Running the String host:port through HostAndPort does some validation for us.
    this.heliosClient = HeliosClient.newBuilder()
            .setUser(username)
            .setEndpoints("http://" +
                    HostAndPort.fromString(dockerHost.address() + ":" + heliosPort))
            .build();
  }

  private DockerHost containerDockerHostFromEnv() {
    if (isBoot2Docker(dockerInfo())) {
      return DockerHost.from(DefaultDockerClient.DEFAULT_UNIX_ENDPOINT, null);
    } else {
      return DockerHost.fromEnv();
    }
  }

  private Boolean isBoot2Docker(final Info dockerInfo) {
    return dockerInfo.operatingSystem().contains(BOOT2DOCKER_SIGNATURE);
  }

  private Info dockerInfo() {
    try {
      return this.dockerClient.info();
    } catch (DockerException | InterruptedException e) {
      // There's not a lot we can do if Docker is unreachable.
      throw new AssertionError(e);
    }
  }

  private List<String> containerEnv() {
    final HashSet<String> env = new HashSet<String>();
    env.add("DOCKER_HOST=" + containerDockerHost.bindURI().toString());
    if (!isNullOrEmpty(containerDockerHost.dockerCertPath())) {
      env.add("DOCKER_CERT_PATH=/certs");
    }
    return ImmutableList.copyOf(env);
  }

  private List<String> containerBinds() {
    final HashSet<String> binds = new HashSet<String>();
    if (containerDockerHost.bindURI().getScheme().equals("unix")) {
      binds.add(containerDockerHost.bindURI().getSchemeSpecificPart() + ":" +
              containerDockerHost.bindURI().getSchemeSpecificPart());
    }
    if (!isNullOrEmpty(containerDockerHost.dockerCertPath())) {
      binds.add(containerDockerHost.dockerCertPath() + ":/certs");
    }
    return ImmutableList.copyOf(binds);
  }

  private void assertDockerReachableFromContainer() throws HeliosDeploymentException {
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
    try {
      dockerClient.startContainer(creation.id());
      exit = dockerClient.waitContainer(creation.id());
    } catch (DockerException | InterruptedException e) {
      killContainer(creation.id());
      removeContainer(creation.id());
      throw new HeliosDeploymentException("helios-solo probe container failed", e);
    }

    if (exit.statusCode() != 0) {
      removeContainer(creation.id());
      throw new HeliosDeploymentException(String.format(
              "Docker was not reachable (curl exit status %d) using DOCKER_HOST=%s and "
                      + "DOCKER_CERT_PATH=%s from within a container. Please ensure that "
                      + "DOCKER_HOST contains a full hostname or IP address, not localhost, "
                      + "127.0.0.1, etc.",
              exit.statusCode(),
              containerDockerHost.bindURI(),
              containerDockerHost.dockerCertPath()));
    }

    removeContainer(creation.id());
  }

  private List<String> probeCommand(final String probeName) {
    final List<String> cmd = new ArrayList<String>(ImmutableList.of("curl", "-f"));
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

  // TODO(negz): Merge with dockerReachableFromContainer() ?
  private String containerGateway() throws HeliosDeploymentException {
    final ContainerConfig containerConfig = ContainerConfig.builder()
            .env(env)
            .hostConfig(HostConfig.builder().build())
            .image(PROBE_IMAGE)
            .cmd(ImmutableList.of("sh", "-c", "while true;do sleep 10;done"))
            .build();

    final ContainerCreation creation;
    try {
      dockerClient.pull(PROBE_IMAGE);
      creation = dockerClient.createContainer(containerConfig);
    } catch (DockerException | InterruptedException e) {
      throw new HeliosDeploymentException("helios-solo gateway probe container creation failed", e);
    }

    try {
      dockerClient.startContainer(creation.id());
      final String gateway = dockerClient.inspectContainer(creation.id())
              .networkSettings().gateway();
      killContainer(creation.id());
      removeContainer(creation.id());
      return gateway;
    } catch (DockerException | InterruptedException e) {
      killContainer(creation.id());
      removeContainer(creation.id());
      throw new HeliosDeploymentException("helios-solo gateway probe failed", e);
    }
  }

  private String deploySolo(final String heliosHost) throws HeliosDeploymentException {
    //TODO(negz): Don't make this.env immutable so early?
    final List<String> env = new ArrayList<String>();
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

  private String getHeliosMasterPort(final String containerId, final int port)
          throws HeliosDeploymentException {
    final String heliosPort = String.format("%d/tcp", port);
    try {
      final NetworkSettings settings = dockerClient.inspectContainer(containerId).networkSettings();
      for (Map.Entry<String, List<PortBinding>> entry : settings.ports().entrySet()) {
        if (entry.getKey().equals(heliosPort)) {
          for (PortBinding portBinding : entry.getValue()) {
            return portBinding.hostPort();
          }
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

  public HeliosClient client() {
    return this.heliosClient;
  }

  public void close() {
    killContainer(heliosContainerId);
    removeContainer(heliosContainerId);
    this.dockerClient.close();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder fromEnv() {
    try {
      DefaultDockerClient.fromEnv().uri();
      return builder().dockerClient(DefaultDockerClient.fromEnv().build());
    } catch (DockerCertificateException e) {
      // TODO(negz): Just propagate this rather than return null and fail later on checkNotNull?
      log.error("unable to create Docker client from environment", e);
      return builder();
    }
  }

  public static class Builder {
    private DockerClient dockerClient;
    private DockerHost dockerHost;
    private DockerHost containerDockerHost;
    private String namespace;
    private String heliosUsername;

    public Builder dockerClient(final DockerClient dockerClient) {
      this.dockerClient = dockerClient;
      return this;
    }

    public Builder dockerHost(final DockerHost dockerHost) {
      this.dockerHost = dockerHost;
      return this;
    }

    public Builder containerDockerHost(final DockerHost dockerHost) {
      this.containerDockerHost = containerDockerHost;
      return this;
    }

    public Builder namespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder heliosUsername(final String username) {
      this.heliosUsername = username;
      return this;
    }

    public HeliosDeployment build() {
      return new HeliosSoloDeployment(this);
    }
  }
}

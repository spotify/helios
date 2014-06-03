package com.spotify.helios.agent.docker;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import com.spotify.helios.agent.docker.messages.Container;
import com.spotify.helios.agent.docker.messages.ContainerConfig;
import com.spotify.helios.agent.docker.messages.ContainerCreation;
import com.spotify.helios.agent.docker.messages.ContainerExit;
import com.spotify.helios.agent.docker.messages.ContainerInfo;
import com.spotify.helios.agent.docker.messages.HostConfig;
import com.spotify.helios.agent.docker.messages.ImageInfo;

import java.util.List;

public interface DockerClient {

  List<Container> listContainers(ListContainersParam... params)
      throws DockerException, InterruptedException;

  ContainerInfo inspectContainer(String containerId)
      throws DockerException, InterruptedException;

  ImageInfo inspectImage(String image)
      throws DockerException, InterruptedException;

  void pull(String image)
      throws DockerException, InterruptedException;

  ContainerCreation createContainer(ContainerConfig config)
      throws DockerException, InterruptedException;

  ContainerCreation createContainer(ContainerConfig config, String name)
      throws DockerException, InterruptedException;

  void startContainer(String containerId)
      throws DockerException, InterruptedException;

  void startContainer(String containerId, HostConfig hostConfig)
      throws DockerException, InterruptedException;

  ContainerExit waitContainer(String containerId)
      throws DockerException, InterruptedException;

  void killContainer(String containerId)
      throws DockerException, InterruptedException;

  void removeContainer(String containerId)
      throws DockerException, InterruptedException;

  void removeContainer(String containerId, boolean removeVolumes)
      throws DockerException, InterruptedException;

  LogStream logs(String containerId, LogsParameter... params)
      throws DockerException, InterruptedException;

  public static enum LogsParameter {
    FOLLOW,
    STDOUT,
    STDERR,
    TIMESTAMPS,
  }

  public static class ListContainersParam {

    final Multimap<String, String> params = ArrayListMultimap.create();

    private final String name;
    private final String value;

    public ListContainersParam(final String name, final String value) {
      this.name = name;
      this.value = value;
    }

    public String name() {
      return name;
    }

    public String value() {
      return value;
    }

    /**
     * Show all containers. Only running containers are shown by default
     */
    public static ListContainersParam allContainers() {
      return allContainers(true);
    }

    /**
     * Show all containers. Only running containers are shown by default
     */
    public static ListContainersParam allContainers(final boolean all) {
      return create("all", String.valueOf(all));
    }

    /**
     * Show <code>limit</code> last created containers, include non-running ones.
     */
    public static ListContainersParam limitContainers(final Integer limit) {
      return create("limit", String.valueOf(limit));
    }

    /**
     * Show only containers created since id, include non-running ones.
     */
    public static ListContainersParam containersCreatedSince(final String id) {
      return create("since", String.valueOf(id));
    }

    /**
     * Show only containers created before id, include non-running ones.
     */
    public static ListContainersParam containersCreatedBefore(final String id) {
      return create("before", String.valueOf(id));
    }

    /**
     * Show the containers sizes.
     */
    public static ListContainersParam withContainerSizes(final Boolean size) {
      return create("size", String.valueOf(size));
    }

    public static ListContainersParam create(final String name, final String value) {
      return new ListContainersParam(name, value);
    }
  }
}

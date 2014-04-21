package com.spotify.helios.agent;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerCreateResponse;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.ImageInspectResponse;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MonitoredDockerClient {
  private final AsyncDockerClient client;
  private final SupervisorMetrics metrics;
  private final RiemannFacade riemannFacade;
  private final int shortRequestTimeout;
  private final int longRequestTimeout;
  private final int extraLongRequestTimeout;

  public MonitoredDockerClient(AsyncDockerClient client, SupervisorMetrics metrics,
                               RiemannFacade riemannFacade, int shortRequestTimeout,
                               int longRequestTimeout, int extraLongRequestTimeout) {
    this.client = client;
    this.riemannFacade = riemannFacade;
    this.metrics = metrics;
    this.shortRequestTimeout = shortRequestTimeout;
    this.longRequestTimeout = longRequestTimeout;
    this.extraLongRequestTimeout = extraLongRequestTimeout;
  }

  public ContainerInspectResponse inspectContainer(String containerId) throws DockerException, InterruptedException {
    try {
      return futureValue(client.inspectContainer(containerId));
    } catch (DockerException e) {
      checkForDockerTimeout(e, "inspectContainer");
      throw propagateDockerException(e);
    }
  }

  /** just like inspectContainer, but returns null if container doesn't exist */
  public ContainerInspectResponse safeInspectContainer(final String containerId)
      throws DockerException {
    try {
      return inspectContainer(containerId);
    } catch (InterruptedException e) {
      Thread.interrupted(); // or else we get a cool endless loop of IE's
      throw new DockerException(e);
    } catch (DockerException e) {
      // XXX (dano): checking for string in exception message is a kludge
      if (!e.getMessage().contains("No such container")) {
        throw e;
      }
      return null;
    }
  }

  public ImageInspectResponse inspectImage(final String image) throws DockerException, InterruptedException {
    try {
      return futureValue(client.inspectImage(image));
    } catch (DockerException e) {
      checkForDockerTimeout(e, "inspectImage");
      throw propagateDockerException(e);
    }
  }

  /**
   * A wrapper around {@link AsyncDockerClient#inspectImage(String)} that returns null instead
   * of throwing an exception if the image is missing.
   */
  public ImageInspectResponse safeInspectImage(final String image)
      throws DockerException {
    try {
      return inspectImage(image);
    } catch (InterruptedException e) {
      Thread.interrupted(); // or else we get a cool endless loop of IE's
      throw new DockerException(e);
    } catch (DockerException e) {
      // XXX (dano): checking for string in exception message is a kludge
      if (!e.getMessage().contains("No such image")) {
        throw e;
      }
      return null;
    }
  }

  public ListenableFuture<Void> startContainer(String containerId, HostConfig hostConfig) {
    return client.startContainer(containerId, hostConfig);
  }

  public ContainerCreateResponse createContainer(ContainerConfig containerConfig, String name)
      throws DockerException, InterruptedException {
    try {
      return longFutureValue(client.createContainer(containerConfig, name));
    } catch (DockerException e) {
      checkForDockerTimeout(e, "createContainer");
      throw propagateDockerException(e);
    }

  }

  public ListenableFuture<Integer> waitContainer(String containerId) {
    // can't really put a timeout on this
    return client.waitContainer(containerId);
  }

  public PullClientResponse pull(String image) throws DockerException, InterruptedException {
    try {
      return extraLongFutureValue(client.pull(image));
    } catch (DockerException e) {
      checkForDockerTimeout(e, "pull");
      throw propagateDockerException(e);
    }
  }

  public void kill(String containerId) throws DockerException, InterruptedException {
    try {
      futureValue(client.kill(containerId));
    } catch (DockerException e) {
      checkForDockerTimeout(e, "kill");
      throw propagateDockerException(e);
    }

  }

  private <T> T futureValue(final ListenableFuture<T> future, int seconds) throws DockerException {
    try {
      return Futures.get(future, shortRequestTimeout, TimeUnit.SECONDS, DockerException.class);
    } catch (DockerException e) {
      if (e.getCause().getClass() == TimeoutException.class) {
        future.cancel(true); // should trigger finally blocks in the callable to close clients
      }
      throw e;
    }
  }

  private <T> T futureValue(final ListenableFuture<T> future) throws DockerException {
    return futureValue(future, shortRequestTimeout);
  }

  private <T> T longFutureValue(final ListenableFuture<T> future) throws DockerException {
    return futureValue(future, longRequestTimeout);
  }

  private <T> T extraLongFutureValue(final ListenableFuture<T> future) throws DockerException {
    return futureValue(future, extraLongRequestTimeout);
  }

  public boolean checkForDockerTimeout(DockerException e, final String method) {
    final Throwable cause = e.getCause();
    if (cause != null && cause.getClass() == TimeoutException.class) {
      metrics.dockerTimeout();
      riemannFacade.event()
          .service("helios-agent/docker")
          .tags("docker", "timeout", method)
          .send();
      return true;
    }
    return false;
  }

  public DockerException propagateDockerException(DockerException e)
      throws DockerException, InterruptedException {
    Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
    Throwables.propagateIfInstanceOf(e.getCause(), DockerException.class);
    return e;
  }
}

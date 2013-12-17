/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.agent;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.kpelykh.docker.client.DockerClient;
import com.kpelykh.docker.client.model.Container;
import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerCreateResponse;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.Image;
import com.sun.jersey.api.client.ClientResponse;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AsyncDockerClient {

  private final DockerClientFactory clientFactory;

  private final ListeningExecutorService executor =
      MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

  AsyncDockerClient(final DockerClientFactory clientFactory) {
    this.clientFactory = clientFactory;
  }

  private DockerClient client() {
    return clientFactory.create();
  }

  public ListenableFuture<ContainerInspectResponse> inspectContainer(final String containerId) {
    return executor.submit(new Callable<ContainerInspectResponse>() {
      @Override
      public ContainerInspectResponse call() throws Exception {
        return client().inspectContainer(containerId);
      }
    });
  }

  public ListenableFuture<List<Image>> getImages(final String name) {
    return executor.submit(new Callable<List<Image>>() {
      @Override
      public List<Image> call() throws Exception {
        return client().getImages(name);
      }
    });
  }

  public ListenableFuture<ClientResponse> pull(final String repository) {
    return executor.submit(new Callable<ClientResponse>() {
      @Override
      public ClientResponse call() throws Exception {
        return client().pull(repository);
      }
    });
  }

  public ListenableFuture<ContainerCreateResponse> createContainer(final ContainerConfig config) {
    return executor.submit(new Callable<ContainerCreateResponse>() {
      @Override
      public ContainerCreateResponse call() throws Exception {
        return client().createContainer(config);
      }
    });
  }

  public ListenableFuture<ContainerCreateResponse> createContainer(final ContainerConfig config,
                                                                   final String name) {
    return executor.submit(new Callable<ContainerCreateResponse>() {
      @Override
      public ContainerCreateResponse call() throws Exception {
        return client().createContainer(config, name);
      }
    });
  }

  public ListenableFuture<Void> startContainer(final String containerId) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        client().startContainer(containerId);
        return null;
      }
    });
  }

  public ListenableFuture<Void> startContainer(final String containerId,
                                               final HostConfig hostConfig) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        client().startContainer(containerId, hostConfig);
        return null;
      }
    });
  }

  public ListenableFuture<Integer> waitContainer(final String containerId) {
    return executor.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return client().waitContainer(containerId);
      }
    });
  }

  public ListenableFuture<Void> kill(final String containerId) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        client().kill(containerId);
        return null;
      }
    });
  }

  public Future<List<Container>> listContainers(final boolean allContainers) {
    return executor.submit(new Callable<List<Container>>() {
      @Override
      public List<Container> call() throws Exception {
        return client().listContainers(allContainers);
      }
    });
  }
}

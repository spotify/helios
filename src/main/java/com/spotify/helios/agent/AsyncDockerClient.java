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
import com.kpelykh.docker.client.model.ImageInspectResponse;

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
        DockerClient client = client();
        try {
          return client.inspectContainer(containerId);
        } finally {
          client.closeConnection();
        }
      }
    });
  }

  public ListenableFuture<ImageInspectResponse> inspectImage(final String image) {
    return executor.submit(new Callable<ImageInspectResponse>() {
      @Override
      public ImageInspectResponse call() throws Exception {
        DockerClient client = client();
        try {
          return client.inspectImage(image);
        } finally {
          client.closeConnection();
        }
      }
    });
  }
  public ListenableFuture<List<Image>> getImages(final String name) {
    return executor.submit(new Callable<List<Image>>() {
      @Override
      public List<Image> call() throws Exception {
        DockerClient client = client();
        try {
          return client.getImages(name);
        } finally {
          client.closeConnection();
        }
      }
    });
  }

  public ListenableFuture<PullClientResponse> pull(final String repository) {
    return executor.submit(new Callable<PullClientResponse>() {
      @Override
      public PullClientResponse call() throws Exception {
        DockerClient client = client();
        try {
          return new PullClientResponse(client.pull(repository), client);
        } catch (Exception e) {
          client.closeConnection();
          throw e;
        }
      }
    });
  }

  public ListenableFuture<ContainerCreateResponse> createContainer(final ContainerConfig config) {
    return executor.submit(new Callable<ContainerCreateResponse>() {
      @Override
      public ContainerCreateResponse call() throws Exception {
        DockerClient client = client();
        try {
          return client.createContainer(config);
        } finally {
          client.closeConnection();
        }
      }
    });
  }

  public ListenableFuture<ContainerCreateResponse> createContainer(final ContainerConfig config,
    final String name) {
    return executor.submit(new Callable<ContainerCreateResponse>() {
      @Override
      public ContainerCreateResponse call() throws Exception {
        DockerClient client = client();
        try {
          return client.createContainer(config, name);
        } finally {
          client.closeConnection();
        }
      }
    });
  }

  public ListenableFuture<Void> startContainer(final String containerId) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        DockerClient client = client();
        try {
          client.startContainer(containerId);
          return null;
        } finally {
          client.closeConnection();
        }
      }
    });
  }


  public ListenableFuture<Void> startContainer(final String containerId,
    final HostConfig hostConfig) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        DockerClient client = client();
        try {
          client.startContainer(containerId, hostConfig);
        } finally {
          client.closeConnection();
        }
        return null;
      }
    });
  }

  public ListenableFuture<Integer> waitContainer(final String containerId) {
    return executor.submit(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        DockerClient client = client();
        try {
          return client.waitContainer(containerId);
        } finally {
          client.closeConnection();
        }
      }
    });
  }

  public ListenableFuture<Void> kill(final String containerId) {
    return executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        DockerClient client = client();
        try {
          client.kill(containerId);
        } finally {
          client.closeConnection();
        }
        return null;
      }
    });
  }

  public Future<List<Container>> listContainers(final boolean allContainers) {
    return executor.submit(new Callable<List<Container>>() {
      @Override
      public List<Container> call() throws Exception {
        DockerClient client = client();
        try {
          return client.listContainers(allContainers);
        } finally {
          client.closeConnection();
        }
      }
    });
  }
}

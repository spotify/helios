/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.agent;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.singletonList;

import com.spotify.docker.client.DockerClient;
import com.spotify.helios.serviceregistration.ServiceRegistrar;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * As you might guess, creates {@link TaskRunner}s
 */
public class TaskRunnerFactory {

  private final TaskConfig taskConfig;
  private final DockerClient docker;
  private final Optional<HealthChecker> healthChecker;
  private final ServiceRegistrar registrar;
  private final List<TaskRunner.Listener> listeners;

  public TaskRunnerFactory(final Builder builder) {
    this.taskConfig = checkNotNull(builder.config, "config");
    this.registrar = checkNotNull(builder.registrar, "registrar");
    this.docker = checkNotNull(builder.docker, "docker");
    this.listeners = checkNotNull(builder.listeners, "listeners");
    this.healthChecker = Optional.fromNullable(builder.healthChecker);
  }

  public TaskRunner create(final long delay,
                           final String containerId,
                           final TaskRunner.Listener listener,
                           final int secondsToWaitBeforeKill) {
    final TaskRunner.Listener broadcastingListener =
        new BroadcastingListener(concat(this.listeners, singletonList(listener)));

    return TaskRunner.builder()
        .delayMillis(delay)
        .config(taskConfig)
        .docker(docker)
        .healthChecker(healthChecker.orNull())
        .existingContainerId(containerId)
        .listener(broadcastingListener)
        .registrar(registrar)
        .secondsToWaitBeforeKill(secondsToWaitBeforeKill)
        .imagePuller(DockerClientImagePuller.create(broadcastingListener, docker))
        .build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
    }

    private TaskConfig config;
    private DockerClient docker;
    private HealthChecker healthChecker;
    private ServiceRegistrar registrar;
    private List<TaskRunner.Listener> listeners = Lists.newArrayList();

    public Builder config(final TaskConfig config) {
      this.config = config;
      return this;
    }

    public Builder registrar(final ServiceRegistrar registrar) {
      this.registrar = registrar;
      return this;
    }

    public Builder dockerClient(final DockerClient docker) {
      this.docker = docker;
      return this;
    }

    public Builder healthChecker(final HealthChecker healthChecker) {
      this.healthChecker = healthChecker;
      return this;
    }

    public Builder listener(final TaskRunner.Listener listener) {
      this.listeners.add(listener);
      return this;
    }

    public TaskRunnerFactory build() {
      return new TaskRunnerFactory(this);
    }
  }

  private class BroadcastingListener implements TaskRunner.Listener {

    private final Iterable<TaskRunner.Listener> listeners;

    private BroadcastingListener(final Iterable<TaskRunner.Listener> listeners) {
      this.listeners = listeners;
    }

    @Override
    public void failed(final Throwable t, final String containerError) {
      for (final TaskRunner.Listener l : listeners) {
        l.failed(t, containerError);
      }
    }

    @Override
    public void pulling() {
      for (final TaskRunner.Listener l : listeners) {
        l.pulling();
      }
    }

    @Override
    public void pulled() {
      for (final TaskRunner.Listener l : listeners) {
        l.pulled();
      }
    }

    @Override
    public void pullFailed() {
      for (final TaskRunner.Listener l : listeners) {
        l.pullFailed();
      }
    }

    @Override
    public void creating() {
      for (final TaskRunner.Listener l : listeners) {
        l.creating();
      }
    }

    @Override
    public void created(final String containerId) {
      for (final TaskRunner.Listener l : listeners) {
        l.created(containerId);
      }
    }

    @Override
    public void starting() {
      for (final TaskRunner.Listener l : listeners) {
        l.starting();
      }
    }

    @Override
    public void started() {
      for (final TaskRunner.Listener l : listeners) {
        l.started();
      }
    }

    @Override
    public void healthChecking() {
      for (final TaskRunner.Listener l : listeners) {
        l.healthChecking();
      }
    }

    @Override
    public void running() {
      for (final TaskRunner.Listener l : listeners) {
        l.running();
      }
    }

    @Override
    public void exited(final int code) {
      for (final TaskRunner.Listener l : listeners) {
        l.exited(code);
      }
    }
  }
}

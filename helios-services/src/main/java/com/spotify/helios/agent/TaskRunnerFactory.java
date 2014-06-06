/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.spotify.docker.client.DockerClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.serviceregistration.NopServiceRegistrar;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.statistics.NoopSupervisorMetrics;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;

public class TaskRunnerFactory {

  private final Job job;
  private final TaskConfig taskConfig;
  private final SupervisorMetrics metrics;
  private final DockerClient docker;
  private final FlapController flapController;
  private final ServiceRegistrar registrar;

  private TaskRunnerFactory(final ServiceRegistrar registrar,
                           final Job job,
                           final TaskConfig taskConfig,
                           final SupervisorMetrics metrics,
                           final DockerClient docker,
                           final FlapController flapController) {
    this.registrar = registrar;
    this.job = job;
    this.taskConfig = taskConfig;
    this.metrics = metrics;
    this.docker = docker;
    this.flapController = checkNotNull(flapController);
  }

  public TaskRunnerFactory(final Builder builder) {
    this.registrar = checkNotNull(builder.registrar, "registrar");
    this.job = checkNotNull(builder.job, "job");
    this.taskConfig = checkNotNull(builder.taskConfig, "taskConfig");
    this.metrics = checkNotNull(builder.metrics, "metrics");
    this.docker = checkNotNull(builder.docker, "docker");
    this.flapController = checkNotNull(builder.flapController, "flapController");
  }

  public TaskRunner create(final long delay,
                           final String containerId,
                           final AtomicReference<ThrottleState> throttle,
                           final StatusUpdater statusUpdater) {
    return new TaskRunner(delay, registrar, job, taskConfig, metrics, docker, flapController,
                          throttle, statusUpdater, containerId);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
    }

    private ServiceRegistrar registrar = new NopServiceRegistrar();
    private Job job;
    private TaskConfig taskConfig;
    private SupervisorMetrics metrics = new NoopSupervisorMetrics();
    private DockerClient docker;
    private FlapController flapController;

    public Builder registrar(final ServiceRegistrar registrar) {
      this.registrar = registrar;
      return this;
    }

    public Builder job(final Job job) {
      this.job = job;
      return this;
    }

    public Builder taskConfig(final TaskConfig taskConfig) {
      this.taskConfig = taskConfig;
      return this;
    }

    public Builder metrics(final SupervisorMetrics metrics) {
      this.metrics = metrics;
      return this;
    }

    public Builder dockerClient(final DockerClient docker) {
      this.docker = docker;
      return this;
    }

    public Builder flapController(final FlapController flapController) {
      this.flapController = flapController;
      return this;
    }

    public TaskRunnerFactory build() {
      return new TaskRunnerFactory(this);
    }
  }
}

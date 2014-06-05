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

import com.google.common.base.Supplier;

import com.spotify.docker.client.DockerClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;

public class TaskRunnerFactory {
  private final Job job;
  private final ContainerUtil containerUtil;
  private final SupervisorMetrics metrics;
  private final DockerClient docker;
  private final FlapController flapController;
  private final ServiceRegistrar registrar;

  public TaskRunnerFactory(final ServiceRegistrar registrar,
                           final Job job,
                           final ContainerUtil containerUtil,
                           final SupervisorMetrics metrics,
                           final DockerClient docker,
                           final FlapController flapController) {
    this.registrar = registrar;
    this.job = job;
    this.containerUtil = containerUtil;
    this.metrics = metrics;
    this.docker = docker;
    this.flapController = checkNotNull(flapController);
  }

  public TaskRunner create(final long delay,
                           final Supplier<String> containerIdSupplier,
                           final AtomicReference<ThrottleState> throttle,
                           final StatusUpdater statusUpdater) {
    return new TaskRunner(delay, registrar, job, containerUtil, metrics, docker, flapController,
                          throttle, statusUpdater, containerIdSupplier);
  }
}

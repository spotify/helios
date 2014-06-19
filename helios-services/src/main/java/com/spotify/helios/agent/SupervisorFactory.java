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
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates job supervisors.
 *
 * @see Supervisor
 */
public class SupervisorFactory {

  private final AgentModel model;
  private final DockerClient dockerClient;
  private final String namespace;
  private final Map<String, String> envVars;
  private final ServiceRegistrar registrar;
  private final ContainerDecorator containerDecorator;
  private final String host;
  private final SupervisorMetrics metrics;

  public SupervisorFactory(final AgentModel model, final DockerClient dockerClient,
                           final Map<String, String> envVars,
                           final ServiceRegistrar registrar,
                           final ContainerDecorator containerDecorator,
                           final String host,
                           final SupervisorMetrics supervisorMetrics,
                           final String namespace) {
    this.dockerClient = dockerClient;
    this.namespace = namespace;
    this.model = checkNotNull(model);
    this.envVars = checkNotNull(envVars);
    this.registrar = registrar;
    this.containerDecorator = containerDecorator;
    this.host = host;
    this.metrics = supervisorMetrics;
  }

  /**
   * Create a new application container.
   *
   * @return A new container.
   */
  public Supervisor create(final Job job, final String existingContainerId,
                           final Map<String, Integer> ports,
                           final Supervisor.Listener listener) {
    final RestartPolicy policy = RestartPolicy.newBuilder().build();
    final TaskConfig taskConfig = TaskConfig.builder()
        .host(host)
        .job(job)
        .ports(ports)
        .envVars(envVars)
        .containerDecorator(containerDecorator)
        .namespace(namespace)
        .build();

    final TaskStatus.Builder taskStatus = TaskStatus.newBuilder()
        .setJob(job)
        .setEnv(taskConfig.containerEnv())
        .setPorts(taskConfig.ports());
    final StatusUpdater statusUpdater = new DefaultStatusUpdater(model, taskStatus);
    final FlapController flapController = FlapController.create();
    final TaskMonitor taskMonitor = new TaskMonitor(job.getId(), flapController, statusUpdater);

    final TaskRunnerFactory runnerFactory = TaskRunnerFactory.builder()
        .config(taskConfig)
        .registrar(registrar)
        .dockerClient(dockerClient)
        .listener(taskMonitor)
        .build();

    return Supervisor.newBuilder()
        .setJob(job)
        .setExistingContainerId(existingContainerId)
        .setDockerClient(dockerClient)
        .setRestartPolicy(policy)
        .setMetrics(metrics)
        .setListener(listener)
        .setRunnerFactory(runnerFactory)
        .setStatusUpdater(statusUpdater)
        .setMonitor(taskMonitor)
        .build();
  }
}

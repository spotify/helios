/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.agent;

import static com.google.common.base.Preconditions.checkNotNull;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerHost;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;
import java.io.File;
import java.util.List;
import java.util.Map;

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
  private final List<ContainerDecorator> containerDecorators;
  private final DockerHost dockerHost;
  private final String host;
  private final SupervisorMetrics metrics;
  private final String defaultRegistrationDomain;
  private final List<String> dns;
  private final boolean agentRunningInContainer;

  public SupervisorFactory(final AgentModel model, final DockerClient dockerClient,
                           final Map<String, String> envVars,
                           final ServiceRegistrar registrar,
                           final List<ContainerDecorator> containerDecorators,
                           final DockerHost dockerHost,
                           final String host,
                           final SupervisorMetrics supervisorMetrics,
                           final String namespace,
                           final String defaultRegistrationDomain,
                           final List<String> dns) {
    this.dockerClient = dockerClient;
    this.namespace = namespace;
    this.model = checkNotNull(model, "model");
    this.envVars = checkNotNull(envVars, "envVars");
    this.registrar = registrar;
    this.containerDecorators = containerDecorators;
    this.dockerHost = dockerHost;
    this.host = host;
    this.metrics = supervisorMetrics;
    this.defaultRegistrationDomain = checkNotNull(defaultRegistrationDomain,
        "defaultRegistrationDomain");
    this.dns = checkNotNull(dns, "dns");
    this.agentRunningInContainer = checkIfAgentRunningInContainer();
  }

  private static boolean checkIfAgentRunningInContainer() {
    return new File("/", ".dockerenv").exists();
  }

  /**
   * Create a new application container.
   *
   * @param job                 The job definition.
   * @param existingContainerId ID of existing container.
   * @param ports               The ports.
   * @param listener            The listener.
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
        .containerDecorators(containerDecorators)
        .namespace(namespace)
        .defaultRegistrationDomain(defaultRegistrationDomain)
        .dns(dns)
        .build();

    final TaskStatus.Builder taskStatus = TaskStatus.newBuilder()
        .setJob(job)
        .setEnv(taskConfig.containerEnv())
        .setPorts(taskConfig.ports());
    final StatusUpdater statusUpdater = new DefaultStatusUpdater(model, taskStatus);
    final FlapController flapController = FlapController.create();
    final TaskMonitor taskMonitor = new TaskMonitor(job.getId(), flapController, statusUpdater);

    final HealthChecker healthChecker = HealthCheckerFactory.create(
        taskConfig, dockerClient, dockerHost, agentRunningInContainer);

    final TaskRunnerFactory runnerFactory = TaskRunnerFactory.builder()
        .config(taskConfig)
        .registrar(registrar)
        .dockerClient(dockerClient)
        .healthChecker(healthChecker)
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

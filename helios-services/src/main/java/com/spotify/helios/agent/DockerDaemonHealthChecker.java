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

import com.codahale.metrics.health.HealthCheck;
import com.spotify.docker.client.DockerClient;

/**
 * Checks if helios-agent can talk to Docker daemon.
 */
class DockerDaemonHealthChecker extends HealthCheck {

  private final DockerClient dockerClient;

  DockerDaemonHealthChecker(final DockerClient dockerClient) {
    this.dockerClient = dockerClient;
  }

  @Override
  protected Result check() throws Exception {
    try {
      dockerClient.ping();
      return Result.healthy();
    } catch (final Exception ex) {
      return Result.unhealthy(ex);
    }
  }
}

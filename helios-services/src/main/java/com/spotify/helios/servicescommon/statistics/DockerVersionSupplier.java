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

package com.spotify.helios.servicescommon.statistics;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;

import java.util.function.Supplier;

/**
 * Returns the version from the DockerClient, formatted as a String so that exceptions can be
 * propogated by returning special String values.
 */
public class DockerVersionSupplier implements Supplier<String> {

  private final DockerClient dockerClient;

  public DockerVersionSupplier(final DockerClient dockerClient) {
    this.dockerClient = dockerClient;
  }

  @Override
  public String get() {
    try {
      return dockerClient.version().version();
    } catch (DockerException | InterruptedException e) {
      return "n/a";
    }
  }
}

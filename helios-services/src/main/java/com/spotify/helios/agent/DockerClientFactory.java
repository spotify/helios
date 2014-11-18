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

import com.google.common.base.Throwables;

import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.docker.client.DockerClient;
import com.spotify.helios.servicescommon.RiemannFacade;

import java.nio.file.Path;

import static com.google.common.base.Strings.isNullOrEmpty;

public class DockerClientFactory {
  private AgentConfig config;
  private RiemannFacade riemannFacade;

  public DockerClientFactory(final AgentConfig config, final RiemannFacade riemannFacade) {
    this.config = config;
    this.riemannFacade = riemannFacade;
  }

  public DockerClient getClient() {
    PollingDockerClient client;
    if (isNullOrEmpty(config.getDockerHost().dockerCertPath())) {
      client = new PollingDockerClient(config.getDockerHost().uri());
    } else {
      final Path dockerCertPath = java.nio.file.Paths.get(config.getDockerHost().dockerCertPath());
      final DockerCertificates dockerCertificates;
      try {
        dockerCertificates = new DockerCertificates(dockerCertPath);
      } catch (DockerCertificateException e) {
        throw Throwables.propagate(e);
      }

      client = new PollingDockerClient(config.getDockerHost().uri(), dockerCertificates);
    }
    return MonitoredDockerClient.wrap(riemannFacade, client);
  }   
}

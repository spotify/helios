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
import com.google.common.collect.Lists;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.Container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class Reaper {

  private final Logger log = LoggerFactory.getLogger(Reaper.class);

  private final DockerClient docker;
  private final String prefix;

  public Reaper(final DockerClient docker, final String namespace) {
    this.docker = docker;
    this.prefix = "/" + namespace;
  }

  public void reap(final Supplier<Set<String>> active) throws InterruptedException {
    try {
      reap0(active);
    } catch (DockerException e) {
      log.error("reaping failed", e);
    }
  }

  private void reap0(final Supplier<Set<String>> activeSupplier)
      throws DockerException, InterruptedException {
    final List<String> candidates = Lists.newArrayList();
    final List<Container> containers = docker.listContainers();
    for (Container container : containers) {
      for (String name : container.names()) {
        if (name.startsWith(prefix)) {
          candidates.add(container.id());
        }
      }
    }

    // Get the active set after we've enumerated candidates to ensure that active set is fresh.
    // If the active set is fetched before enumerating candidates it might be stale and we might
    // mistakenly classify a container as not being in the active set.
    final Set<String> active = activeSupplier.get();
    for (final String candidate : candidates) {
      if (!active.contains(candidate)) {
        reap(candidate);
      }
    }
  }

  private void reap(final String containerId) throws InterruptedException, DockerException {
    log.info("reaping {}", containerId);
    docker.killContainer(containerId);
  }
}

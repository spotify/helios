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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.helios.common.descriptors.Job;

import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Bind mounts user-specified volumes into all containers.
 */
public class BindVolumeContainerDecorator implements ContainerDecorator {

  private final List<String> binds;

  public BindVolumeContainerDecorator(final List<String> binds) {
    this.binds = ImmutableList.copyOf(binds);
  }

  public static boolean isValidBind(final String bind) {
    if (isNullOrEmpty(bind)) {
      return false;
    }

    final String[] parts = bind.split(":");

    if (isNullOrEmpty(parts[0])) {
      return false;
    }
    if ((parts.length > 1) && isNullOrEmpty(parts[1])) {
      return false;
    }
    if ((parts.length > 2) && !parts[2].equals("ro") && !parts[2].equals("rw")) {
      return false;
    }
    if (parts.length > 3) {
      return false;
    }

    return true;
  }

  @Override
  public void decorateHostConfig(HostConfig.Builder hostConfig) {
    final List<String> b = Lists.newArrayList();

    if (hostConfig.binds() != null) {
      b.addAll(hostConfig.binds());
    }

    b.addAll(this.binds);

    hostConfig.binds(b);
  }

  @Override
  public void decorateContainerConfig(Job job, ImageInfo imageInfo,
                                      ContainerConfig.Builder containerConfig) {
  }
}

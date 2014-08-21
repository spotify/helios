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

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerExit;
import com.spotify.docker.client.messages.ContainerInfo;

import java.net.URI;

/**
 * A {@link DockerClient} that overrides {@link #waitContainer} to poll instead of block
 * indefinitely.  See the source code for details as to why this needs to exist.
 */
public class PollingDockerClient extends DefaultDockerClient {

  private static final long WAIT_INSPECT_INTERVAL_MILLIS =
      Long.getLong("HELIOS_WAIT_INSPECT_INTERVAL_MILLIS", 5000);

  public PollingDockerClient(final String uri) {
    super(uri);
  }

  public PollingDockerClient(final URI uri) {
    super(uri);
  }

  @Override
  public ContainerExit waitContainer(final String containerId)
      throws DockerException, InterruptedException {
    // XXX (dano): We're doing this poll loop instead of docker.waitContainer() because we saw the
    //             agent hang forever on waitContainer after the socket got into a weird half-open
    //             state where the kernel (netstat/lsof) would only show one end of the connection
    //             and restarting docker would not close the socket. ¯\_(ツ)_/¯
    while (true) {
      final ContainerInfo info = inspectContainer(containerId);
      if (!info.state().running()) {
        return new ContainerExit(info.state().exitCode());
      }
      Thread.sleep(WAIT_INSPECT_INTERVAL_MILLIS);
    }
  }
}

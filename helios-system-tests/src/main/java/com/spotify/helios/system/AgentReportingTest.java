/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.system;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.docker.client.DockerClient;
import com.spotify.helios.Polling;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.DockerVersion;
import com.spotify.helios.common.descriptors.HostStatus;

import org.junit.Test;

import java.util.concurrent.Callable;

public class AgentReportingTest extends SystemTestBase {

  @Test
  public void verifyAgentReportsDockerVersion() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());

    final HeliosClient client = defaultClient();
    final DockerVersion dockerVersion = Polling.await(
        LONG_WAIT_SECONDS, SECONDS, new Callable<DockerVersion>() {
          @Override
          public DockerVersion call() throws Exception {
            final HostStatus status = client.hostStatus(testHost()).get();
            return status == null
                   ? null
                   : status.getHostInfo() == null
                     ? null
                     : status.getHostInfo().getDockerVersion();
          }
        });

    try (final DockerClient dockerClient = getNewDockerClient()) {
      final String expectedDockerVersion = dockerClient.version().version();
      assertThat(dockerVersion.getVersion(), is(expectedDockerVersion));
    }
  }
}

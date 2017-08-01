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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheck;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerRequestException;
import java.net.URI;
import org.junit.Test;

public class DockerDaemonHealthCheckerTest {

  private final DockerClient dockerClient = mock(DockerClient.class);
  private final DockerDaemonHealthChecker sut = new DockerDaemonHealthChecker(dockerClient);

  @Test
  public void testHealthy() throws Exception {
    final HealthCheck.Result result = sut.check();
    assertThat(result.isHealthy(), is(true));
  }

  @Test
  public void testUnhealthy() throws Exception {
    when(dockerClient.ping())
        .thenThrow(new DockerRequestException("GET", new URI("/ping"), 500, null, null));
    final HealthCheck.Result result = sut.check();
    assertThat(result.isHealthy(), is(false));
  }
}

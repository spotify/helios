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

import static java.util.Collections.emptyList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerClient.ExecStartParameter;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.messages.ExecCreation;
import com.spotify.docker.client.messages.ExecState;
import com.spotify.docker.client.messages.Info;
import com.spotify.docker.client.messages.Version;
import com.spotify.helios.agent.HealthCheckerFactory.ExecHealthChecker;
import com.spotify.helios.common.descriptors.ExecHealthCheck;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ExecHealthCheckerTest {

  private static final String CONTAINER_ID = "abc123def";
  private static final String EXEC_ID = "5678";

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  private DockerClient docker;
  private ExecHealthChecker checker;

  @Before
  public void setUp() throws Exception {
    final ExecHealthCheck healthCheck = ExecHealthCheck.of("exit 0");

    final Info info = mock(Info.class);
    when(info.executionDriver()).thenReturn("native-0.2");

    final Version version = mock(Version.class);
    when(version.apiVersion()).thenReturn("1.18");

    final ExecState execState = mock(ExecState.class);
    when(execState.exitCode()).thenReturn(0);

    final LogStream log = mock(LogStream.class);
    when(log.readFully()).thenReturn("");

    docker = mock(DockerClient.class);
    when(docker.info()).thenReturn(info);
    when(docker.version()).thenReturn(version);
    when(docker.execCreate(eq(CONTAINER_ID), any(String[].class),
        (DockerClient.ExecCreateParam) anyVararg()))
        .thenReturn(ExecCreation.create(EXEC_ID, emptyList()));
    when(docker.execStart(eq(EXEC_ID), (ExecStartParameter) anyVararg())).thenReturn(log);
    when(docker.execInspect(EXEC_ID)).thenReturn(execState);

    checker = new ExecHealthChecker(healthCheck, docker);
  }

  @Test
  public void testHealthCheckSuccess() {
    assertThat(checker.check(CONTAINER_ID), is(true));
  }

  @Test
  public void testHealthCheckFailure() throws Exception {
    final ExecState execState = mock(ExecState.class);
    when(execState.exitCode()).thenReturn(2);
    when(docker.execInspect(EXEC_ID)).thenReturn(execState);

    assertThat(checker.check(CONTAINER_ID), is(false));
  }

  @Test
  public void testIncompatibleVersion() throws Exception {
    final Version version = mock(Version.class);
    when(version.apiVersion()).thenReturn("1.15");
    when(docker.version()).thenReturn(version);

    exception.expect(UnsupportedOperationException.class);
    checker.check(CONTAINER_ID);
  }
}

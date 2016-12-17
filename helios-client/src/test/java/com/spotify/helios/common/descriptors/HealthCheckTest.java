/*-
 * -\-\-
 * Helios Client
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

package com.spotify.helios.common.descriptors;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import org.junit.Test;

public class HealthCheckTest {

  @Test
  public void testHttpHealthCheckBuilder() {
    final HttpHealthCheck.Builder builder = HealthCheck.newHttpHealthCheck();

    // Input to setXXX
    final String setPort = "http-admin";
    final String setPath = "/healthcheck";

    // Check setXXX methods
    builder.setPort(setPort);
    builder.setPath(setPath);

    assertEquals("port", setPort, builder.getPort());
    assertEquals("path", setPath, builder.getPath());

    // Check final output
    final HttpHealthCheck healthCheck = builder.build();
    assertEquals("port", setPort, healthCheck.getPort());
    assertEquals("path", setPath, healthCheck.getPath());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHttpWithoutPort() {
    HealthCheck.newHttpHealthCheck().setPath("/healthcheck").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHttpWithoutPath() {
    HealthCheck.newHttpHealthCheck().setPort("http-admin").build();
  }

  @Test
  public void testTcpHealthCheckBuilder() {
    final TcpHealthCheck healthCheck =
        HealthCheck.newTcpHealthCheck().setPort("http-admin").build();
    assertEquals("port", "http-admin", healthCheck.getPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTcpWithoutPort() {
    HealthCheck.newTcpHealthCheck().build();
  }

  @Test
  public void testExecHealthCheckBuilder() {
    final ExecHealthCheck healthCheck = HealthCheck.newExecHealthCheck()
        .setCommand(Collections.singletonList("whoami")).build();
    assertEquals("cmd", healthCheck.getCommand(), ImmutableList.of("whoami"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExecWithoutCmd() {
    HealthCheck.newExecHealthCheck().build();
  }
}

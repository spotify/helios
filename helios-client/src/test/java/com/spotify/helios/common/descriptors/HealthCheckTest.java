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

package com.spotify.helios.common.descriptors;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.HealthCheck.CMD;
import static com.spotify.helios.common.descriptors.HealthCheck.HTTP;
import static com.spotify.helios.common.descriptors.HealthCheck.TCP;
import static org.junit.Assert.assertEquals;

public class HealthCheckTest {

  @Test
  public void verifyBuilderWithHTTP() {
    final HealthCheck.Builder builder = HealthCheck.newBuilder();

    // Input to setXXX
    final String setType = HTTP;
    final String setPort = "http-admin";
    final String setPath = "/healthcheck";

    // Check setXXX methods
    builder.setType(setType);
    builder.setPort(setPort);
    builder.setPath(setPath);

    assertEquals("type", setType, builder.getType());
    assertEquals("port", setPort, builder.getPort());
    assertEquals("path", setPath, builder.getPath());

    // Check final output
    final HealthCheck healthCheck = builder.build();
    assertEquals("type", setType, healthCheck.getType());
    assertEquals("port", setPort, healthCheck.getPort());
    assertEquals("path", setPath, healthCheck.getPath());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHTTPWithoutPort() {
    HealthCheck.newBuilder().setType(HTTP).setPath("/healthcheck").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHTTPWithoutPath() {
    HealthCheck.newBuilder().setType(HTTP).setPort("http-admin").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testHTTPWithCmd() {
    HealthCheck.newBuilder().setType(HTTP).setPort("http-admin").setPath("/health")
        .setCmd("whoami").build();
  }

  @Test
  public void testBuilderWithTCP() {
    final HealthCheck healthCheck =
        HealthCheck.newBuilder().setType(TCP).setPort("http-admin").build();
    assertEquals("type", TCP, healthCheck.getType());
    assertEquals("port", "http-admin", healthCheck.getPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTCPWithoutPort() {
    HealthCheck.newBuilder().setType(TCP).build();
  }

  @Test
  public void testBuilderWithCMD() {
    final HealthCheck healthCheck = HealthCheck.newBuilder().setType(CMD).setCmd("whoami").build();
    assertEquals("type", CMD, healthCheck.getType());
    assertEquals("cmd", "whoami", healthCheck.getCmd());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCMDWithoutCmd() {
    HealthCheck.newBuilder().setType(CMD).build();
  }
}

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

package com.spotify.helios.common;

import org.junit.Test;

import static com.spotify.helios.common.VersionCompatibility.Status;
import static org.junit.Assert.assertEquals;

public class VersionCompatibilityTest {

  @Test
  public void test() {
    PomVersion server = PomVersion.parse("1.3.9");
    assertEquals(Status.EQUAL, VersionCompatibility.getStatus(server, server));
    assertEquals(Status.COMPATIBLE, VersionCompatibility.getStatus(
        server, PomVersion.parse("1.3.10")));
    assertEquals(Status.COMPATIBLE, VersionCompatibility.getStatus(
        server, PomVersion.parse("1.2.8")));
    assertEquals(Status.MAYBE, VersionCompatibility.getStatus(
        server, PomVersion.parse("1.4.8")));
    assertEquals(Status.INCOMPATIBLE, VersionCompatibility.getStatus(
        server, PomVersion.parse("9.0.0")));
  }
}

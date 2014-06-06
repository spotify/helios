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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.TaskStatus.State;

import org.junit.Test;

import java.util.Map;

import static com.spotify.helios.common.descriptors.Goal.START;
import static org.junit.Assert.assertEquals;

public class TaskStatusTest {
  private static final Job JOB = Job.newBuilder()
      .setCommand(ImmutableList.of("BOGUS"))
      .setImage("IMAGE")
      .setName("NAME")
      .setVersion("VERSION")
      .build();
  private static final Map<String, String> ENV = ImmutableMap.<String, String>builder()
      .put("VAR", "VALUE")
      .build();
  private static final TaskStatus STATUS = TaskStatus.newBuilder()
      .setContainerId("CONTAINER_ID")
      .setGoal(START)
      .setJob(JOB)
      .setState(State.RUNNING)
      .setEnv(ENV)
      .setThrottled(ThrottleState.NO)
      .build();

  @Test
  public void testSerializationOfEnvironment() throws Exception {

    byte[] bytes = Json.asBytes(STATUS);

    final TaskStatus read = Json.read(bytes, TaskStatus.class);
    assertEquals(1, read.getEnv().size());
    assertEquals("VALUE", read.getEnv().get("VAR"));
  }

  @Test
  public void testToBuilderAndBackEnvironment() throws Exception {
    TaskStatus s = STATUS.asBuilder().build();
    assertEquals(1, s.getEnv().size());
    assertEquals("VALUE", s.getEnv().get("VAR"));
  }
}

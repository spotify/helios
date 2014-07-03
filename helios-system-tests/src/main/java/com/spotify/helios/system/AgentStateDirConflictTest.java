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

package com.spotify.helios.system;

import com.google.common.util.concurrent.Service;

import com.spotify.helios.ZooKeeperStandaloneServerManager;
import com.spotify.helios.agent.AgentMain;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.file.Files;
import java.nio.file.Path;

public class AgentStateDirConflictTest {
  @Rule
  public ExpectedException exception = ExpectedException.none();

  private Thread.UncaughtExceptionHandler dueh;
  private Path stateDir;
  private AgentMain first;
  private AgentMain second;
  private ZooKeeperStandaloneServerManager zk;

  @Before
  public void setup() throws Exception {
    zk = new ZooKeeperStandaloneServerManager();
    dueh = Thread.getDefaultUncaughtExceptionHandler();

    Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(final Thread t, final Throwable e) {}
    });

    stateDir = Files.createTempDirectory("helios-agent-conflict-test");

    first = makeAgent("first");
    second = makeAgent("second");
  }

  private AgentMain makeAgent(String name) throws ArgumentParserException {
    return new AgentMain("-vvvv",
                        "--no-log-setup",
                        "--no-http",
                        "--name=" + name,
                        "--zk=" + zk.connectString(),
                        "--state-dir", stateDir.toString(),
                        "--domain", "");
  }

  @After
  public void teardown() throws Exception {
    FileUtils.deleteQuietly(stateDir.toFile());
    stopQuietly(first);
    stopQuietly(second);
    Thread.setDefaultUncaughtExceptionHandler(dueh);
    zk.stop();
  }

  private void stopQuietly(final Service service) {
    if (service == null) {
      return;
    }
    try {
      service.stopAsync().awaitTerminated();
    } catch (Exception ignore) {
    }
  }

  @Test
  public void test() throws Exception {
    first.startAsync().awaitRunning();
    exception.expect(IllegalStateException.class);
    second.startAsync().awaitRunning();
  }
}

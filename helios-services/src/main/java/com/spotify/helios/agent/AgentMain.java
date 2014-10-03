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

import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.servicescommon.ServiceMain;

import net.sourceforge.argparse4j.inf.ArgumentParserException;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Instantiates and runs helios agent.
 */
public class AgentMain extends ServiceMain {

  private final AgentConfig agentConfig;
  private AgentService service;

  public AgentMain(final String... args) throws ArgumentParserException {
    this(new AgentParser(args));
  }

  public AgentMain(final AgentParser parser) {
    this(parser.getAgentConfig(), parser.getLoggingConfig());
  }

  public AgentMain(final AgentConfig agentConfig, final LoggingConfig loggingConfig) {
    super(loggingConfig, agentConfig.getSentryDsn());
    this.agentConfig = agentConfig;
  }

  @Override
  protected void startUp() throws Exception {
    service = new AgentService(agentConfig, createEnvironment("helios-agent"));
    service.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    if (service != null) {
      service.stopAsync().awaitTerminated();
    }
  }

  public static void main(final String... args) {
    final AgentMain main;
    final AtomicBoolean exitSignalTriggered = new AtomicBoolean(false);

    final AtomicReference<SignalHandler> existingExitHandler =
        new AtomicReference<SignalHandler>(null);

    final SignalHandler handler = new SignalHandler() {
      @Override
      public void handle(Signal signal) {
        if (exitSignalTriggered.get()) {
          System.err.println("Exiting with extreme prejudice due to " + signal);
          // Really exit
          Runtime.getRuntime().halt(0);
        } else {
          System.err.println("Attempting gentle exit on " + signal);
          exitSignalTriggered.set(true);
          existingExitHandler.get().handle(signal);
        }
      }
    };
    existingExitHandler.set(Signal.handle(new Signal("INT"), handler));
    Signal.handle(new Signal("TERM"), handler);

    try {
      main = new AgentMain(args);
    } catch (ArgumentParserException e) {
      e.printStackTrace();
      System.exit(1);
      return; // never get here, but this lets java know for sure we won't continue
    }
    try {
      main.startAsync().awaitRunning();
      main.awaitTerminated();
    } catch (Throwable e) {
      try {
        main.shutDown();
      } catch (Exception e1) {
        System.err.println("Error shutting down");
        e1.printStackTrace();
        System.err.println("Originating exception follows");
      }
      e.printStackTrace();
      System.exit(1);
    }
    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }
}

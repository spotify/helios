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
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import com.yammer.dropwizard.validation.Validator;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

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
    final Environment environment = new Environment("helios-agent", agentConfig,
        new ObjectMapperFactory(), new Validator());
    service = new AgentService(agentConfig, environment);
    service.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    if (service != null) {
      service.stopAsync().awaitTerminated();
    }
  }

  public static void main(final String... args) {
    try {
      final AgentMain main = new AgentMain(args);
      main.startAsync().awaitRunning();
      main.awaitTerminated();
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(1);
    }
    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }

}

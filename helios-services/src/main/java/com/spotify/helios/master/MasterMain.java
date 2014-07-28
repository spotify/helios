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

package com.spotify.helios.master;

import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.servicescommon.ServiceMain;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactory;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactoryImpl;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import com.yammer.dropwizard.validation.Validator;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instantiates and runs the helios master. We do our own bootstrapping instead of using
 * {@link com.yammer.dropwizard.config.Bootstrap} because we want more control over logging etc.
 */
public class MasterMain extends ServiceMain {

  private static final Logger log = LoggerFactory.getLogger(MasterMain.class);

  private final MasterConfig masterConfig;
  private final CuratorClientFactory curatorClientFactory;
  private MasterService service;

  public MasterMain(final String[] args) throws ArgumentParserException {
    this(new CuratorClientFactoryImpl(), new MasterParser(args));
  }

  public MasterMain(final CuratorClientFactory curatorClientFactory,
                    final String[] args) throws ArgumentParserException {
    this(curatorClientFactory, new MasterParser(args));
  }

  public MasterMain(final CuratorClientFactory curatorClientFactory,
                    final MasterParser parser) {
    this(curatorClientFactory, parser.getMasterConfig(), parser.getLoggingConfig());
  }

  public MasterMain(final CuratorClientFactory curatorClientFactory,
                    final MasterConfig masterConfig,
                    final LoggingConfig loggingConfig) {
    super(loggingConfig, masterConfig.getSentryDsn());
    this.masterConfig = masterConfig;
    this.curatorClientFactory = curatorClientFactory;
  }

  @Override
  protected void startUp() throws Exception {
    final Environment environment = new Environment("helios-master", masterConfig,
                                                    new ObjectMapperFactory(), new Validator());
    service = new MasterService(masterConfig, environment, curatorClientFactory);
    service.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    service.stopAsync().awaitTerminated();
  }

  public static void main(final String... args) {
    try {
      final MasterMain main = new MasterMain(args);
      main.startAsync().awaitRunning();
      main.awaitTerminated();
    } catch (Throwable e) {
      log.error("Uncaught exception", e);
      System.exit(1);
    }
    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }
}

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

import com.spotify.helios.servicescommon.ServiceParser;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.file.Path;

import static net.sourceforge.argparse4j.impl.Arguments.fileType;

/**
 * Parses command-line arguments to produce the {@link MasterConfig}.
 */
public class MasterParser extends ServiceParser {

  private final MasterConfig masterConfig;

  private final Namespace options;
  private Argument httpArg;
  private Argument adminArg;
  private Argument authPluginArg;

  public MasterParser(final String... args) throws ArgumentParserException {
    super("helios-master", "Spotify Helios Master", args);

    options = getNamespace();
    final InetSocketAddress httpAddress = parseSocketAddress(options.getString(httpArg.getDest()));

    final MasterConfig config = new MasterConfig()
        .setZooKeeperConnectString(getZooKeeperConnectString())
        .setZooKeeperSessionTimeoutMillis(getZooKeeperSessionTimeoutMillis())
        .setZooKeeperConnectionTimeoutMillis(getZooKeeperConnectionTimeoutMillis())
        .setZooKeeperNamespace(getZooKeeperNamespace())
        .setZooKeeperClusterId(getZooKeeperClusterId())
        .setNoZooKeeperMasterRegistration(getNoZooKeeperRegistration())
        .setDomain(getDomain())
        .setName(getName())
        .setStatsdHostPort(getStatsdHostPort())
        .setRiemannHostPort(getRiemannHostPort())
        .setInhibitMetrics(getInhibitMetrics())
        .setSentryDsn(getSentryDsn())
        .setServiceRegistryAddress(getServiceRegistryAddress())
        .setServiceRegistrarPlugin(getServiceRegistrarPlugin())
        .setAdminPort(options.getInt(adminArg.getDest()))
        .setHttpEndpoint(httpAddress)
        .setKafkaBrokers(getKafkaBrokers())
        .setStateDirectory(getStateDirectory())
        .setAuthPlugin(getAuthPlugin())
        .setAuthSecret(System.getenv("HELIOS_AUTH_SECRET"));

    this.masterConfig = config;
  }

  private Path getAuthPlugin() {
    final File plugin = options.get(authPluginArg.getDest());
    return plugin != null ? plugin.toPath() : null;
  }

  @Override
  protected void addArgs(final ArgumentParser parser) {
    httpArg = parser.addArgument("--http")
        .setDefault("http://0.0.0.0:5801")
        .help("http endpoint");

    adminArg = parser.addArgument("--admin")
        .type(Integer.class)
        .setDefault(5802)
        .help("admin http port");

    authPluginArg = parser.addArgument("--auth-plugin")
        .type(fileType().verifyExists().verifyCanRead())
        .help("Path to authenticator plugin.");
  }

  public MasterConfig getMasterConfig() {
    return masterConfig;
  }
}

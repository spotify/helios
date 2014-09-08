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

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import com.spotify.helios.servicescommon.DockerHost;
import com.spotify.helios.servicescommon.ServiceParser;
import com.yammer.dropwizard.config.HttpConfiguration;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.net.InetAddresses.isInetAddress;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * Parses and processes command-line arguments to produce the {@link AgentConfig}.
 */
public class AgentParser extends ServiceParser {

  private final AgentConfig agentConfig;

  private Argument noHttpArg;
  private Argument httpArg;
  private Argument adminArg;
  private Argument stateDirArg;
  private Argument dockerArg;
  private Argument envArg;
  private Argument syslogRedirectToArg;
  private Argument portRangeArg;
  private Argument agentIdArg;
  private Argument dnsArg;

  public AgentParser(final String... args) throws ArgumentParserException {
    super("helios-agent", "Spotify Helios Agent", args);

    final Namespace options = getNamespace();
    final DockerHost dockerHost = DockerHost.from(options.getString(dockerArg.getDest()));

    final List<List<String>> env = options.getList(envArg.getDest());
    final Map<String, String> envVars = Maps.newHashMap();
    if (env != null) {
      for (final List<String> group : env) {
        for (final String s : group) {
          final String[] parts = s.split("=", 2);
          if (parts.length != 2) {
            throw new IllegalArgumentException("Bad environment variable: " + s);
          }
          envVars.put(parts[0], parts[1]);
        }
      }
    }

    final InetSocketAddress httpAddress = parseSocketAddress(options.getString(httpArg.getDest()));

    final String portRangeString = options.getString(portRangeArg.getDest());
    final List<String> parts = Splitter.on(':').splitToList(portRangeString);
    if (parts.size() != 2) {
      throw new IllegalArgumentException("Bad port range: " + portRangeString);
    }
    final int start;
    final int end;
    try {
      start = Integer.valueOf(parts.get(0));
      end = Integer.valueOf(parts.get(1));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Bad port range: " + portRangeString);
    }
    if (end <= start) {
      throw new IllegalArgumentException("Bad port range: " + portRangeString);
    }

    this.agentConfig = new AgentConfig()
        .setName(getName())
        .setZooKeeperConnectionString(getZooKeeperConnectString())
        .setZooKeeperSessionTimeoutMillis(getZooKeeperSessionTimeoutMillis())
        .setZooKeeperConnectionTimeoutMillis(getZooKeeperConnectionTimeoutMillis())
        .setZooKeeperNamespace(getZooKeeperNamespace())
        .setDomain(getDomain())
        .setEnvVars(envVars)
        .setDockerHost(dockerHost)
        .setInhibitMetrics(getInhibitMetrics())
        .setRedirectToSyslog(options.getString(syslogRedirectToArg.getDest()))
        .setStateDirectory(Paths.get(options.getString(stateDirArg.getDest())))
        .setStatsdHostPort(getStatsdHostPort())
        .setRiemannHostPort(getRiemannHostPort())
        .setPortRange(start, end)
        .setSentryDsn(getSentryDsn())
        .setServiceRegistryAddress(getServiceRegistryAddress())
        .setServiceRegistrarPlugin(getServiceRegistrarPlugin());

    final String explicitId = options.getString(agentIdArg.getDest());
    if (explicitId != null) {
      agentConfig.setId(explicitId);
    } else {
      final byte[] idBytes = new byte[20];
      new SecureRandom().nextBytes(idBytes);
      agentConfig.setId(base16().encode(idBytes));
    }

    final boolean noHttp = options.getBoolean(noHttpArg.getDest());

    if (noHttp) {
      agentConfig.setHttpConfiguration(null);
    } else {
      final HttpConfiguration http = agentConfig.getHttpConfiguration();
      http.setPort(httpAddress.getPort());
      http.setBindHost(httpAddress.getHostString());
      http.setAdminPort(options.getInt(adminArg.getDest()));
    }

    final List<String> dns = options.getList(dnsArg.getDest());
    if (!dns.isEmpty()) {
      for (String d : dns) {
        if (!isInetAddress(d)) {
          throw new IllegalArgumentException("Invalid IP address " + d);
        }
      }
    }
    agentConfig.setDns(dns);
  }

  @Override
  protected void addArgs(final ArgumentParser parser) {
    noHttpArg = parser.addArgument("--no-http")
        .action(storeTrue())
        .setDefault(false)
        .help("disable http server");

    httpArg = parser.addArgument("--http")
        .setDefault("http://0.0.0.0:5803")
        .help("http endpoint");

    adminArg = parser.addArgument("--admin")
        .type(Integer.class)
        .setDefault(5804)
        .help("admin http port");

    agentIdArg = parser.addArgument("--id")
        .help("Agent unique ID. Generated and persisted on first run if not specified.");

    stateDirArg = parser.addArgument("--state-dir")
        .setDefault(".")
        .help("Directory for persisting agent state locally.");

    dockerArg = parser.addArgument("--docker")
        .setDefault(DockerHost.fromEnv().host())
        .help("docker endpoint");

    envArg = parser.addArgument("--env")
        .action(append())
        .setDefault(new ArrayList<String>())
        .nargs("+")
        .help("Specify environment variables that will pass down to all containers");

    syslogRedirectToArg = parser.addArgument("--syslog-redirect-to")
        .help("redirect container's stdout/stderr to syslog running at host:port");

    portRangeArg = parser.addArgument("--port-range")
        .setDefault("20000:32768")
        .help("Port allocation range, start:end (end exclusive).");

    dnsArg = parser.addArgument("--dns")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("Dns servers to use.");

  }

  public AgentConfig getAgentConfig() {
    return agentConfig;
  }

}

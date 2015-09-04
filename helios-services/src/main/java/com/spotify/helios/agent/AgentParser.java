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

import com.spotify.helios.servicescommon.DockerHost;
import com.spotify.helios.servicescommon.ServiceParser;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.io.BaseEncoding.base16;
import static com.google.common.net.InetAddresses.isInetAddress;
import static com.spotify.helios.agent.BindVolumeContainerDecorator.isValidBind;
import static com.spotify.helios.cli.Utils.argToStringMap;
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
  private Argument dockerArg;
  private Argument dockerCertPathArg;
  private Argument envArg;
  private Argument syslogRedirectToArg;
  private Argument portRangeArg;
  private Argument agentIdArg;
  private Argument dnsArg;
  private Argument bindArg;
  private Argument labelsArg;
  private Argument zkRegistrationTtlMinutesArg;

  public AgentParser(final String... args) throws ArgumentParserException {
    super("helios-agent", "Spotify Helios Agent", args);

    final Namespace options = getNamespace();
    final DockerHost dockerHost = DockerHost.from(
        options.getString(dockerArg.getDest()),
        options.getString(dockerCertPathArg.getDest()));

    final Map<String, String> envVars = argToStringMap(options, envArg);
    final Map<String, String> labels;
    try {
      labels = argToStringMap(options, labelsArg);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage() +
                                         "\nLabels need to be in the format key=value.");
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
        .setZooKeeperClusterId(getZooKeeperClusterId())
        .setZooKeeperRegistrationTtlMinutes(options.getInt(zkRegistrationTtlMinutesArg.getDest()))
        .setDomain(getDomain())
        .setEnvVars(envVars)
        .setDockerHost(dockerHost)
        .setInhibitMetrics(getInhibitMetrics())
        .setRedirectToSyslog(options.getString(syslogRedirectToArg.getDest()))
        .setStateDirectory(getStateDirectory())
        .setStatsdHostPort(getStatsdHostPort())
        .setRiemannHostPort(getRiemannHostPort())
        .setPortRange(start, end)
        .setSentryDsn(getSentryDsn())
        .setServiceRegistryAddress(getServiceRegistryAddress())
        .setServiceRegistrarPlugin(getServiceRegistrarPlugin())
        .setAdminPort(options.getInt(adminArg.getDest()))
        .setHttpEndpoint(httpAddress)
        .setNoHttp(options.getBoolean(noHttpArg.getDest()))
        .setKafkaBrokers(getKafkaBrokers())
        .setLabels(labels);

    final String explicitId = options.getString(agentIdArg.getDest());
    if (explicitId != null) {
      agentConfig.setId(explicitId);
    } else {
      final byte[] idBytes = new byte[20];
      new SecureRandom().nextBytes(idBytes);
      agentConfig.setId(base16().encode(idBytes));
    }

    final List<String> dns = options.getList(dnsArg.getDest());
    if (!dns.isEmpty()) {
      for (final String d : dns) {
        if (!isInetAddress(d)) {
          throw new IllegalArgumentException("Invalid IP address " + d);
        }
      }
    }
    agentConfig.setDns(dns);

    final List<String> binds = options.getList(bindArg.getDest());
    if (!binds.isEmpty()) {
      for (final String b : binds) {
        if (!isValidBind(b)) {
          throw new IllegalArgumentException("Invalid bind " + b);
        }
      }
    }
    agentConfig.setBinds(binds);
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

    dockerArg = parser.addArgument("--docker")
        .setDefault(DockerHost.fromEnv().host())
        .help("docker endpoint");

    dockerCertPathArg = parser.addArgument("--docker-cert-path")
        .setDefault(DockerHost.fromEnv().dockerCertPath())
        .help("directory containing client.pem and client.key for connecting to Docker over HTTPS");

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

    bindArg = parser.addArgument("--bind")
        .action(append())
        .setDefault(new ArrayList<String>())
        .help("volumes to bind to all containers");

    labelsArg = parser.addArgument("--labels")
        .action(append())
        .setDefault(new ArrayList<String>())
        .nargs("+")
        .help("labels to apply to this agent. Labels need to be in the format key=value.");

    zkRegistrationTtlMinutesArg = parser.addArgument("--zk-registration-ttl")
        .type(Integer.class)
        .setDefault(10)
        .help("Number of minutes that this agent must be DOWN (i.e. not periodically check-in with "
              + "ZooKeeper) before another agent with the same hostname but lacking the "
              + "registration ID of this one can automatically deregister this one and register "
              + "itself. This is useful when the agent loses its registration ID and you don't "
              + "want to waste time debugging why the master lists your agent as constantly DOWN.");
  }

  public AgentConfig getAgentConfig() {
    return agentConfig;
  }

}

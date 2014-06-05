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

package com.spotify.helios.servicescommon;

import com.google.common.io.CharStreams;

import com.spotify.helios.common.LoggingConfig;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.sourceforge.argparse4j.impl.Arguments.SUPPRESS;
import static net.sourceforge.argparse4j.impl.Arguments.fileType;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class ServiceParser {

  private final Namespace options;

  private final Argument nameArg;
  private final Argument sentryDsnArg;
  private final Argument domainArg;
  private final Argument serviceRegistryArg;
  private final Argument serviceRegistrarPluginArg;
  private final Argument zooKeeperConnectStringArg;
  private final Argument zooKeeperSessiontimeoutArg;
  private final Argument zooKeeperConnectiontimeoutArg;
  private final Argument noMetricsArg;
  private final Argument statsdHostPortArg;
  private final Argument riemannHostPortArg;
  private final Argument verboseArg;
  private final Argument syslogArg;
  private final Argument logconfigArg;
  private final Argument noLogSetupArg;

  public ServiceParser(final String programName, final String description, final String... args)
      throws ArgumentParserException {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser(programName)
        .defaultHelp(true)
        .description(description);

    nameArg = parser.addArgument("--name")
        .setDefault(getHostName())
        .help("hostname to register as");

    domainArg = parser.addArgument("--domain")
        .help("Service registration domain.");

    serviceRegistryArg = parser.addArgument("--service-registry")
        .help("Service registry address. Overrides domain.");

    serviceRegistrarPluginArg = parser.addArgument("--service-registrar-plugin")
        .type(fileType().verifyExists().verifyCanRead())
        .help("Service registration plugin.");

    zooKeeperConnectStringArg = parser.addArgument("--zk")
        .setDefault("localhost:2181")
        .help("zookeeper connection string");

    zooKeeperSessiontimeoutArg = parser.addArgument("--zk-session-timeout")
        .type(Integer.class)
        .setDefault((int) SECONDS.toMillis(60))
        .help("zookeeper session timeout");

    zooKeeperConnectiontimeoutArg = parser.addArgument("--zk-connection-timeout")
        .type(Integer.class)
        .setDefault((int) SECONDS.toMillis(15))
        .help("zookeeper connection timeout");

    noMetricsArg = parser.addArgument("--no-metrics")
        .setDefault(SUPPRESS)
        .action(storeTrue())
        .help("Turn off all collection and reporting of metrics");

    statsdHostPortArg = parser.addArgument("--statsd-host-port")
        .setDefault((String) null)
        .help("host:port of where to send statsd metrics "
              + "(to be useful, --no-metrics must *NOT* be specified)");

    riemannHostPortArg = parser.addArgument("--riemann-host-port")
        .setDefault((String) null)
        .help("host:port of where to send riemann events and metrics "
              + "(to be useful, --no-metrics must *NOT* be specified)");

    verboseArg = parser.addArgument("-v", "--verbose")
        .action(Arguments.count());

    syslogArg = parser.addArgument("--syslog")
        .help("Log to syslog.")
        .action(storeTrue());

    logconfigArg = parser.addArgument("--logconfig")
        .type(fileType().verifyExists().verifyCanRead())
        .help("Logback configuration file.");

    noLogSetupArg = parser.addArgument("--no-log-setup")
        .action(storeTrue())
        .help(SUPPRESS);

    sentryDsnArg = parser.addArgument("--sentry-dsn")
        .setDefault((String) null)
        .help("The sentry data source name");

    addArgs(parser);

    try {
      this.options = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      throw e;
    }
  }

  protected void addArgs(final ArgumentParser parser) {
  }

  public Namespace getNamespace() {
    return options;
  }

  public LoggingConfig getLoggingConfig() {
    return new LoggingConfig(options.getInt(verboseArg.getDest()),
                             options.getBoolean(syslogArg.getDest()),
                             (File) options.get(logconfigArg.getDest()),
                             options.getBoolean(noLogSetupArg.getDest()));
  }

  public String getDomain() {
    return options.getString(domainArg.getDest());
  }

  public String getServiceRegistryAddress() {
    return options.getString(serviceRegistryArg.getDest());
  }

  public Path getServiceRegistrarPlugin() {
    final File plugin = (File) options.get(serviceRegistrarPluginArg.getDest());
    return plugin != null ? plugin.toPath() : null;
  }

  public String getZooKeeperConnectString() {
    return options.getString(zooKeeperConnectStringArg.getDest());
  }

  public String getSentryDsn() {
    return options.getString(sentryDsnArg.getDest());
  }

  public Boolean getInhibitMetrics() {
    return fromNullable(options.getBoolean(noMetricsArg.getDest())).or(false);
  }

  public String getName() {
    return options.getString(nameArg.getDest());
  }

  public String getRiemannHostPort() {
    return options.getString(riemannHostPortArg.getDest());
  }

  public String getStatsdHostPort() {
    return options.getString(statsdHostPortArg.getDest());
  }

  public int getZooKeeperConnectionTimeoutMillis() {
    return options.getInt(zooKeeperConnectiontimeoutArg.getDest());
  }

  public int getZooKeeperSessionTimeoutMillis() {
    return options.getInt(zooKeeperSessiontimeoutArg.getDest());
  }

  private static String getHostName() {
    return exec("uname -n").trim();
  }

  private static String exec(final String command) {
    try {
      final Process process = Runtime.getRuntime().exec(command);
      return CharStreams.toString(new InputStreamReader(process.getInputStream(), UTF_8));
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  protected InetSocketAddress parseSocketAddress(final String addressString) {
    final InetSocketAddress address;
    try {
      final URI u = new URI(addressString);
      address = new InetSocketAddress(u.getHost(), u.getPort());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Bad address: " + addressString, e);
    }
    return address;
  }
}

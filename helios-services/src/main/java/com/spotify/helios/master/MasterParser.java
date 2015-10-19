/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.spotify.helios.auth.ServerAuthenticationConfig;
import com.spotify.helios.common.PomVersion;
import com.spotify.helios.servicescommon.ServiceParser;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentChoice;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;
import java.net.InetSocketAddress;

import static net.sourceforge.argparse4j.impl.Arguments.fileType;

/**
 * Parses command-line arguments to produce the {@link MasterConfig}.
 */
public class MasterParser extends ServiceParser {

  private final MasterConfig masterConfig;

  private Argument httpArg;
  private Argument adminArg;

  // authentication-related arguments:
  private Argument authenticationScheme;
  private Argument authenticationMinimumVersion;
  private Argument authenticationPluginsPathArg;

  public MasterParser(final String... args) throws ArgumentParserException {
    super("helios-master", "Spotify Helios Master", args);

    final Namespace options = getNamespace();
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
        .setStateDirectory(getStateDirectory());

    final String authSchemeName = options.getString(authenticationScheme.getDest());
    if (authSchemeName != null) {
      ServerAuthenticationConfig authConfig = new ServerAuthenticationConfig();
      authConfig.setEnabledScheme(authSchemeName);

      final File pluginPath = options.get(authenticationPluginsPathArg.getDest());
      if (pluginPath != null) {
        authConfig.setPluginsPath(pluginPath.toPath());
      }

      final String minVersion = options.getString(authenticationMinimumVersion.getDest());
      if (minVersion != null) {
        authConfig.setMinimumRequiredVersion(minVersion);
      }

      config.setAuthenticationConfig(authConfig);
    }
    this.masterConfig = config;
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

    authenticationScheme = parser.addArgument("--auth-scheme")
        .metavar("SCHEMENAME")
        .help("Name of authentication scheme to use. "
              + "Setting this flag enables authentication in Helios. "
              + "'crtauth' support is built into Helios. "
              + "For other values, --auth-plugin must also be set.");

    authenticationMinimumVersion = parser.addArgument("--auth-minimum-version")
        .help("Set to a version string to only require authentication for versions >= that version."
              + " Otherwise all requests require authentication. Only valid when --auth-enabled"
              + " is set.")
        .metavar("VERSION-STRING")
        .choices(new VersionStringChoice());

    authenticationPluginsPathArg = parser.addArgument("--auth-plugin")
        .type(fileType().verifyExists().verifyCanRead())
        .help("Path to authenticator plugin.");
  }

  public MasterConfig getMasterConfig() {
    return masterConfig;
  }

  private static class VersionStringChoice implements ArgumentChoice {

    @Override
    public boolean contains(Object val) {
      final String value = String.valueOf(val);
      try {
        PomVersion.parse(value);
        return true;
      } catch (RuntimeException e) {
        return false;
      }
    }

    @Override
    public String textualFormat() {
      return "a version string like 'x.y.z'";
    }
  }
}

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

import com.spotify.helios.servicescommon.ServiceParser;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;

/**
 * Parses command-line arguments to produce the {@link MasterConfig}.
 */
public class MasterParser extends ServiceParser {

  private static final String ZK_MASTER_PASSWORD_ENVVAR = "HELIOS_ZK_MASTER_PASSWORD";

  private final MasterConfig masterConfig;

  private Argument httpArg;
  private Argument adminArg;
  private Argument zkAclAgentDigest;
  private Argument zkAclMasterPassword;

  public MasterParser(final String... args) throws ArgumentParserException {
    super("helios-master", "Spotify Helios Master", args);

    final Namespace options = getNamespace();
    final InetSocketAddress httpAddress = parseSocketAddress(options.getString(httpArg.getDest()));

    String masterPassword = System.getenv(ZK_MASTER_PASSWORD_ENVVAR);
    if (masterPassword == null) {
      masterPassword = options.getString(zkAclMasterPassword.getDest());
    }

    final MasterConfig config = new MasterConfig()
        .setZooKeeperConnectString(getZooKeeperConnectString())
        .setZooKeeperSessionTimeoutMillis(getZooKeeperSessionTimeoutMillis())
        .setZooKeeperConnectionTimeoutMillis(getZooKeeperConnectionTimeoutMillis())
        .setZooKeeperNamespace(getZooKeeperNamespace())
        .setZooKeeperClusterId(getZooKeeperClusterId())
        .setNoZooKeeperMasterRegistration(getNoZooKeeperRegistration())
        .setZooKeeperEnableAcls(getZooKeeperEnableAcls())
        .setZookeeperAclAgentUser(getZooKeeperAclAgentUser())
        .setZooKeeperAclAgentDigest(options.getString(zkAclAgentDigest.getDest()))
        .setZookeeperAclMasterUser(getZooKeeperAclMasterUser())
        .setZooKeeperAclMasterPassword(masterPassword)
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

    zkAclAgentDigest = parser.addArgument("--zk-acl-agent-digest")
        .type(String.class);

    zkAclMasterPassword = parser.addArgument("--zk-acl-master-password")
        .type(String.class)
        .help("ZooKeeper master password (for ZooKeeper ACLs). If the "
              + ZK_MASTER_PASSWORD_ENVVAR
              + " environment variable is present this argument is ignored.");
  }

  public MasterConfig getMasterConfig() {
    return masterConfig;
  }
}

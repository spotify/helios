/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.master;

import com.google.common.collect.ImmutableSet;
import com.spotify.helios.servicescommon.CommonConfiguration;
import com.spotify.helios.servicescommon.FastForwardConfig;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Set;

/**
 * The collection of the configuration info of the master.
 */
public class MasterConfig extends CommonConfiguration<MasterConfig> {

  // TODO (dano): defaults

  private String domain;
  private String zooKeeperConnectString;
  private String name;
  private boolean inhibitMetrics;
  private String statsdHostPort;
  private String riemannHostPort;
  private String serviceRegistryAddress;
  private String sentryDsn;
  private Path serviceRegistrarPlugin;
  private int zooKeeperSessionTimeoutMillis;
  private int zooKeeperConnectionTimeoutMillis;
  private String zooKeeperClusterId;
  private boolean noZooKeeperMasterRegistration;
  private InetSocketAddress adminEndpoint;
  private InetSocketAddress httpEndpoint;
  private Path stateDirectory;
  private boolean zooKeeperEnableAcls;
  private String zookeeperAclAgentUser;
  private String zooKeeperAclAgentDigest;
  private String zookeeperAclMasterUser;
  private String zooKeeperAclMasterPassword;
  private long agentReapingTimeout;
  private long jobRetention;
  private FastForwardConfig fastForwardConfig;
  private Set<String> whitelistedCapabilities;
  private boolean jobHistoryReapingEnabled;

  public String getDomain() {
    return domain;
  }

  public MasterConfig setDomain(final String domain) {
    this.domain = domain;
    return this;
  }

  public MasterConfig setName(final String name) {
    this.name = name;
    return this;
  }

  public String getName() {
    return name;
  }

  public String getZooKeeperConnectionString() {
    return zooKeeperConnectString;
  }

  public MasterConfig setZooKeeperConnectString(final String zooKeeperConnectString) {
    this.zooKeeperConnectString = zooKeeperConnectString;
    return this;
  }

  public MasterConfig setZooKeeperSessionTimeoutMillis(final int timeoutMillis) {
    this.zooKeeperSessionTimeoutMillis = timeoutMillis;
    return this;
  }

  public int getZooKeeperSessionTimeoutMillis() {
    return zooKeeperSessionTimeoutMillis;
  }

  public MasterConfig setZooKeeperConnectionTimeoutMillis(final int timeoutMillis) {
    this.zooKeeperConnectionTimeoutMillis = timeoutMillis;
    return this;
  }

  public int getZooKeeperConnectionTimeoutMillis() {
    return zooKeeperConnectionTimeoutMillis;
  }

  public String getZooKeeperClusterId() {
    return zooKeeperClusterId;
  }

  public MasterConfig setZooKeeperClusterId(String zooKeeperClusterId) {
    this.zooKeeperClusterId = zooKeeperClusterId;
    return this;
  }

  public boolean getNoZooKeeperMasterRegistration() {
    return noZooKeeperMasterRegistration;
  }

  public MasterConfig setNoZooKeeperMasterRegistration(boolean noZooKeeperMasterRegistration) {
    this.noZooKeeperMasterRegistration = noZooKeeperMasterRegistration;
    return this;
  }

  public MasterConfig setInhibitMetrics(boolean inhibit) {
    this.inhibitMetrics = inhibit;
    return this;
  }

  public boolean isInhibitMetrics() {
    return inhibitMetrics;
  }

  public MasterConfig setStatsdHostPort(String hostPort) {
    this.statsdHostPort = hostPort;
    return this;
  }

  public String getStatsdHostPort() {
    return statsdHostPort;
  }

  public MasterConfig setRiemannHostPort(String hostPort) {
    this.riemannHostPort = hostPort;
    return this;
  }

  public String getRiemannHostPort() {
    return riemannHostPort;
  }

  public MasterConfig setServiceRegistryAddress(final String address) {
    this.serviceRegistryAddress = address;
    return this;
  }

  public String getServiceRegistryAddress() {
    return serviceRegistryAddress;
  }

  public String getSentryDsn() {
    return sentryDsn;
  }

  public MasterConfig setSentryDsn(String sentryDsn) {
    this.sentryDsn = sentryDsn;
    return this;
  }

  public Path getServiceRegistrarPlugin() {
    return serviceRegistrarPlugin;
  }

  public MasterConfig setServiceRegistrarPlugin(final Path serviceRegistrarPlugin) {
    this.serviceRegistrarPlugin = serviceRegistrarPlugin;
    return this;
  }

  public MasterConfig setAdminEndpoint(InetSocketAddress adminEndpoint) {
    this.adminEndpoint = adminEndpoint;
    return this;
  }

  public MasterConfig setHttpEndpoint(InetSocketAddress httpEndpoint) {
    this.httpEndpoint = httpEndpoint;
    return this;
  }

  public Path getStateDirectory() {
    return stateDirectory;
  }

  public MasterConfig setStateDirectory(final Path stateDirectory) {
    this.stateDirectory = stateDirectory;
    return this;
  }

  public InetSocketAddress getAdminEndpoint() {
    return adminEndpoint;
  }

  public InetSocketAddress getHttpEndpoint() {
    return httpEndpoint;
  }

  public MasterConfig setZooKeeperEnableAcls(final boolean zooKeeperEnableAcls) {
    this.zooKeeperEnableAcls = zooKeeperEnableAcls;
    return this;
  }

  public boolean isZooKeeperEnableAcls() {
    return zooKeeperEnableAcls;
  }

  public MasterConfig setZooKeeperAclAgentDigest(final String zooKeeperAclAgentDigest) {
    this.zooKeeperAclAgentDigest = zooKeeperAclAgentDigest;
    return this;
  }

  public String getZooKeeperAclAgentDigest() {
    return zooKeeperAclAgentDigest;
  }

  public MasterConfig setZooKeeperAclMasterPassword(final String zooKeeperAclMasterPassword) {
    this.zooKeeperAclMasterPassword = zooKeeperAclMasterPassword;
    return this;
  }

  public String getZooKeeperAclMasterPassword() {
    return zooKeeperAclMasterPassword;
  }

  public String getZookeeperAclMasterUser() {
    return zookeeperAclMasterUser;
  }

  public MasterConfig setZookeeperAclMasterUser(final String zookeeperAclMasterUser) {
    this.zookeeperAclMasterUser = zookeeperAclMasterUser;
    return this;
  }

  public String getZookeeperAclAgentUser() {
    return zookeeperAclAgentUser;
  }

  public MasterConfig setZookeeperAclAgentUser(final String zookeeperAclAgentUser) {
    this.zookeeperAclAgentUser = zookeeperAclAgentUser;
    return this;
  }

  public MasterConfig setAgentReapingTimeout(final long agentReapingTimeout) {
    this.agentReapingTimeout = agentReapingTimeout;
    return this;
  }

  public long getAgentReapingTimeout() {
    return agentReapingTimeout;
  }

  public MasterConfig setJobRetention(final long jobRetention) {
    this.jobRetention = jobRetention;
    return this;
  }

  public long getJobRetention() {
    return jobRetention;
  }

  public FastForwardConfig getFfwdConfig() {
    return this.fastForwardConfig;
  }

  public MasterConfig setFfwdConfig(FastForwardConfig config) {
    this.fastForwardConfig = config;
    return this;
  }

  public Set<String> getWhitelistedCapabilities() {
    return whitelistedCapabilities;
  }

  public MasterConfig setWhitelistedCapabilities(final Set<String> whitelistedCapabilities) {
    this.whitelistedCapabilities = ImmutableSet.copyOf(whitelistedCapabilities);
    return this;
  }

  public boolean isJobHistoryReapingEnabled() {
    return jobHistoryReapingEnabled;
  }

  public MasterConfig setJobHistoryReapingEnabled(final boolean jobHistoryReapingEnabled) {
    this.jobHistoryReapingEnabled = jobHistoryReapingEnabled;
    return this;
  }
}

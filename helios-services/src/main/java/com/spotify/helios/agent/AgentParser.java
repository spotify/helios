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

package com.spotify.helios.agent;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.BaseEncoding.base16;
import static com.spotify.helios.cli.Utils.argToStringMap;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.fileType;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.net.InetAddresses;
import com.spotify.docker.client.DockerHost;
import com.spotify.helios.servicescommon.ServiceParser;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;


/**
 * Parses and processes command-line arguments to produce the {@link AgentConfig}.
 */
public class AgentParser extends ServiceParser {

  private static final String ZK_AGENT_PASSWORD_ENVVAR = "HELIOS_ZK_AGENT_PASSWORD";

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
  private Argument addHostArg;
  private Argument labelsArg;
  private Argument zkRegistrationTtlMinutesArg;
  private Argument zkAclMasterDigest;
  private Argument zkAclAgentPassword;
  private Argument disableJobHistory;
  private Argument connectionPoolSize;
  private Argument googleCloudCredentialsFile;
  private Argument useGoogleDefaultApplicationCredentials;

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
      throw new IllegalArgumentException(e.getMessage()
                                         + "\nLabels need to be in the format key=value.");
    }

    final InetSocketAddress httpAddress = parseSocketAddress(options.getString(httpArg.getDest()));
    final InetSocketAddress adminAddress = parseSocketAddress(
        options.getString(adminArg.getDest()));

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

    String agentPassword = System.getenv(ZK_AGENT_PASSWORD_ENVVAR);
    if (agentPassword == null) {
      agentPassword = options.getString(zkAclAgentPassword.getDest());
    }

    this.agentConfig = new AgentConfig()
        .setName(getName())
        .setZooKeeperConnectionString(getZooKeeperConnectString())
        .setZooKeeperSessionTimeoutMillis(getZooKeeperSessionTimeoutMillis())
        .setZooKeeperConnectionTimeoutMillis(getZooKeeperConnectionTimeoutMillis())
        .setZooKeeperClusterId(getZooKeeperClusterId())
        .setZooKeeperRegistrationTtlMinutes(options.getInt(zkRegistrationTtlMinutesArg.getDest()))
        .setZooKeeperEnableAcls(getZooKeeperEnableAcls())
        .setZookeeperAclMasterUser(getZooKeeperAclMasterUser())
        .setZooKeeperAclMasterDigest(options.getString(zkAclMasterDigest.getDest()))
        .setZookeeperAclAgentUser(getZooKeeperAclAgentUser())
        .setZooKeeperAclAgentPassword(agentPassword)
        .setDomain(getDomain())
        .setEnvVars(envVars)
        .setDockerHost(dockerHost)
        .setInhibitMetrics(getInhibitMetrics())
        .setRedirectToSyslog(options.getString(syslogRedirectToArg.getDest()))
        .setStateDirectory(getStateDirectory())
        .setStatsdHostPort(getStatsdHostPort())
        .setPortRange(start, end)
        .setSentryDsn(getSentryDsn())
        .setServiceRegistryAddress(getServiceRegistryAddress())
        .setServiceRegistrarPlugin(getServiceRegistrarPlugin())
        .setAdminAddress(adminAddress)
        .setHttpEndpoint(httpAddress)
        .setNoHttp(options.getBoolean(noHttpArg.getDest()))
        .setKafkaBrokers(getKafkaBrokers())
        .setPubsubPrefixes(getPubsubPrefixes())
        .setLabels(labels)
        .setFfwdConfig(ffwdConfig(options))
        .setJobHistoryDisabled(options.getBoolean(disableJobHistory.getDest()))
        .setConnectionPoolSize(firstNonNull(options.getInt(connectionPoolSize.getDest()), -1));

    final String explicitId = options.getString(agentIdArg.getDest());
    if (explicitId != null) {
      agentConfig.setId(explicitId);
    } else {
      final byte[] idBytes = new byte[20];
      new SecureRandom().nextBytes(idBytes);
      agentConfig.setId(base16().encode(idBytes));
    }

    agentConfig.setDns(validateArgument(
        options.getList(dnsArg.getDest()),
        InetAddresses::isInetAddress,
        arg -> "Invalid IP address " + arg));

    agentConfig.setBinds(validateArgument(
        options.getList(bindArg.getDest()),
        BindVolumeContainerDecorator::isValidBind,
        arg -> "Invalid bind " + arg));

    agentConfig.setExtraHosts(validateArgument(
        options.getList(addHostArg.getDest()),
        AddExtraHostContainerDecorator::isValidArg,
        arg -> "Invalid ExtraHost " + arg));

    // options for GCR
    final File googleAccountCredentials =
        options.get(this.googleCloudCredentialsFile.getDest());

    final boolean useGoogleDefaultCredentials =
        options.getBoolean(this.useGoogleDefaultApplicationCredentials.getDest());

    if (useGoogleDefaultCredentials || googleAccountCredentials != null) {
      try {
        agentConfig.setGoogleCredentials(
            loadGoogleCredentials(useGoogleDefaultCredentials, googleAccountCredentials));
      } catch (IOException e) {
        throw new IllegalArgumentException("Cannot setup Google Container Registry credentials", e);
      }
    }
  }

  private GoogleCredentials loadGoogleCredentials(
      final boolean useDefaultCredentials,
      final File credentialsFile)
      throws ArgumentParserException, IOException {

    if (useDefaultCredentials && credentialsFile != null) {
      final String msg = String.format("Cannot set both %s and %s",
          this.googleCloudCredentialsFile.getDest(),
          this.useGoogleDefaultApplicationCredentials.getDest());
      throw new IllegalArgumentException(msg);
    }

    if (useDefaultCredentials) {
      return GoogleCredentials.getApplicationDefault();
    }
    try (final FileInputStream stream = new FileInputStream(credentialsFile)) {
      return GoogleCredentials.fromStream(stream);
    }
  }

  /**
   * Verifies that all entries in the Collection satisfy the predicate. If any do not, throw an
   * IllegalArgumentException with the specified message for the first invalid entry.
   */
  @VisibleForTesting
  protected static <T> List<T> validateArgument(List<T> list, Predicate<T> predicate,
                                                Function<T, String> msgFn) {

    final Optional<T> firstInvalid = list.stream()
        .filter(predicate.negate())
        .findAny();

    if (firstInvalid.isPresent()) {
      throw new IllegalArgumentException(firstInvalid.map(msgFn).get());
    }
    return list;
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
        .setDefault("http://0.0.0.0:5804")
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
        .setDefault(new ArrayList<>())
        .nargs("+")
        .help("Specify environment variables that will pass down to all containers");

    syslogRedirectToArg = parser.addArgument("--syslog-redirect-to")
        .help("redirect container's stdout/stderr to syslog running at host:port");

    portRangeArg = parser.addArgument("--port-range")
        .setDefault("20000:32768")
        .help("Port allocation range, start:end (end exclusive).");

    dnsArg = parser.addArgument("--dns")
        .action(append())
        .setDefault(new ArrayList<>())
        .help("Dns servers to use.");

    bindArg = parser.addArgument("--bind")
        .action(append())
        .setDefault(new ArrayList<>())
        .help("volumes to bind to all containers");

    addHostArg = parser.addArgument("--add-host")
        .action(append())
        .setDefault(new ArrayList<>())
        .help("extra hosts to add to /etc/hosts of created containers, in form `host:ip`. "
              + "See docker documentation for --add-host for more info.");

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

    zkAclMasterDigest = parser.addArgument("--zk-acl-master-digest")
        .type(String.class);

    zkAclAgentPassword = parser.addArgument("--zk-acl-agent-password")
        .type(String.class)
        .help("ZooKeeper agent password (for ZooKeeper ACLs). If the "
              + ZK_AGENT_PASSWORD_ENVVAR
              + " environment variable is present this argument is ignored.");

    disableJobHistory = parser.addArgument("--disable-job-history")
        .action(storeTrue())
        .setDefault(false)
        .help("If specified, the agent won't write job histories to ZooKeeper.");

    connectionPoolSize = parser.addArgument("--docker-connection-pool-size")
        .type(Integer.class)
        .help("Size of the Docker socket connection pool.");

    googleCloudCredentialsFile = parser.addArgument("--docker-gcp-account-credentials")
        .type(fileType().verifyExists().verifyCanRead())
        .help("When set, helios-agent will configure the docker-client to use the Google Cloud "
              + "user or service account whose credentials are contained in the specified file for "
              + "pulling images from Google Container Registry.");

    useGoogleDefaultApplicationCredentials =
        parser.addArgument("--docker-use-gcp-application-default-credentials")
            .action(storeTrue())
            .help("When set, helios-agent will configure the docker-client to use the Google Cloud "
                  + "Application Default Credentials for pulling images from "
                  + "Google Container Registry.");
  }

  public AgentConfig getAgentConfig() {
    return agentConfig;
  }

}

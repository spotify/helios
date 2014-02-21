package com.spotify.helios.agent;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;

import com.spotify.helios.servicescommon.ServiceParser;
import com.yammer.dropwizard.config.HttpConfiguration;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static net.sourceforge.argparse4j.impl.Arguments.SUPPRESS;
import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class AgentParser extends ServiceParser {

  private final AgentConfig agentConfig;

  public AgentParser(final String... args) throws ArgumentParserException {
    super("helios-agent", "Spotify Helios Agent", args);

    final Namespace options = getNamespace();
    final String uriString = options.getString("docker");
    try {
      new URI(uriString);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Bad docker endpoint: " + uriString, e);
    }

    final List<List<String>> env = options.getList("env");
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
    final InetSocketAddress httpAddress = parseSocketAddress(options.getString("http"));

    final String portRangeString = options.getString("port_range");
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
        .setName(options.getString("name"))
        .setZooKeeperConnectionString(options.getString("zk"))
        .setZooKeeperSessionTimeoutMillis(options.getInt("zk_session_timeout"))
        .setZooKeeperConnectionTimeoutMillis(options.getInt("zk_connection_timeout"))
        .setSite(options.getString("site"))
        .setEnvVars(envVars)
        .setDockerEndpoint(options.getString("docker"))
        .setInhibitMetrics(Objects.equal(options.getBoolean("no_metrics"), true))
        .setRedirectToSyslog(options.getString("syslog_redirect_to"))
        .setStateDirectory(Paths.get(options.getString("state_dir")))
        .setStatsdHostPort(options.getString("statsd_host_port"))
        .setRiemannHostPort(options.getString("riemann_host_port"))
        .setNamelessEndpoint(options.getString("nameless"))
        .setPortRange(start, end)
        .setSentryDsn(options.getString("sentry_dsn"));

    final boolean noHttp = options.getBoolean("no_http");

    if (noHttp) {
      agentConfig.setHttpConfiguration(null);
    } else {
      final HttpConfiguration http = agentConfig.getHttpConfiguration();
      http.setPort(httpAddress.getPort());
      http.setBindHost(httpAddress.getHostString());
      http.setAdminPort(options.getInt("admin"));
    }
  }

  @Override
  protected void addArgs(final ArgumentParser parser) {
    parser.addArgument("--no-http")
        .action(storeTrue())
        .setDefault(false)
        .help("disable http server");

    parser.addArgument("--http")
        .setDefault("http://0.0.0.0:5803")
        .help("http endpoint");

    parser.addArgument("--admin")
        .type(Integer.class)
        .setDefault(5804)
        .help("admin http port");

    parser.addArgument("--state-dir")
        .setDefault(".")
        .help("Directory for persisting agent state locally.");

    parser.addArgument("--docker")
        .setDefault("http://localhost:4160")
        .help("docker endpoint");

    parser.addArgument("--env")
        .action(append())
        .setDefault(new ArrayList<String>())
        .nargs("+")
        .help("Specify environment variables that will pass down to all containers");

    parser.addArgument("--syslog-redirect-to")
        .help("redirect container's stdout/stderr to syslog running at host:port");

    parser.addArgument("--no-metrics")
        .setDefault(SUPPRESS)
        .action(storeTrue())
        .help("Turn off all collection and reporting of metrics");

    parser.addArgument("--statsd-host-port")
        .type(String.class)
        .help("host:port of where to send statsd metrics "
            + "(to be useful, --no-metrics must *NOT* be specified)");

    parser.addArgument("--riemann-host-port")
        .setDefault((String) null)
        .help("host:port of where to send riemann events and metrics "
            + "(to be useful, --no-metrics must *NOT* be specified)");

    parser.addArgument("--port-range")
        .setDefault("40000:49152")
        .help("Port allocation range, start:end (end exclusive).");

    parser.addArgument("--sentry-dsn")
        .setDefault((String) null)
        .help("The sentry data source name");

  }

  public AgentConfig getAgentConfig() {
    return agentConfig;
  }

}

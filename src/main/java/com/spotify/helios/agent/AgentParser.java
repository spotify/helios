package com.spotify.helios.agent;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;

import com.spotify.helios.common.ServiceParser;

import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Throwables.propagate;

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

    final String name = options.getString("name");

    final List<String> env = options.getList("env");
    final Map<String, String> envVars = Maps.newHashMap();
    if (env != null) {
      for (final String s : env) {
        final List<String> parts = Splitter.on('=').splitToList(s);
        if (parts.size() != 2) {
          throw new IllegalArgumentException("Bad environment variable: " + s);
        }
        envVars.put(parts.get(0), parts.get(1));
      }
    }

    agentConfig = new AgentConfig()
        .setName(name)
        .setZooKeeperConnectionString(options.getString("zk"))
        .setZooKeeperSessionTimeoutMillis(options.getInt("zk_session_timeout"))
        .setZooKeeperConnectionTimeoutMillis(options.getInt("zk_connection_timeout"))
        .setSite(options.getString("site"))
        .setMuninReporterPort(options.getInt("munin_port"))
        .setEnvVars(envVars)
        .setDockerEndpoint(options.getString("docker"));
  }

  @Override
  protected void addArgs(final ArgumentParser parser) {
    parser.addArgument("--name")
        .setDefault(getHostName())
        .help("agent name");

    parser.addArgument("--munin-port")
        .type(Integer.class)
        .setDefault(4952)
        .help("munin port (0 = disabled)");

    parser.addArgument("--docker")
        .setDefault("http://localhost:4160")
        .help("docker endpoint");

    parser.addArgument("--env")
        .nargs("+")
        .help("Specify environment variables that will pass down to all containers");
  }

  private static String getHostName() {
    return exec("uname -n").trim();
  }

  private static String exec(final String command) {
    try {
      final Process process = Runtime.getRuntime().exec(command);
      return CharStreams.toString(new InputStreamReader(process.getInputStream()));
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  public AgentConfig getAgentConfig() {
    return agentConfig;
  }

}

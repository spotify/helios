package com.spotify.helios.agent;

import com.spotify.helios.common.Defaults;
import com.spotify.helios.common.LoggingConfig;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.json.JSONException;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import static com.google.common.base.Throwables.propagate;
import static net.sourceforge.argparse4j.impl.Arguments.*;

public class AgentParser {

  private final Namespace options;
  private final LoggingConfig loggingConfig;
  private final AgentConfig agentConfig;

  public AgentParser(final String... args) throws ArgumentParserException, JSONException, IOException {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("sphelios-agent")
        .defaultHelp(true)
        .description("Spotify Helios Agent");

    addArgs(parser);

    try {
      this.options = parser.parseArgs(args);

      this.loggingConfig = new LoggingConfig(options.getInt("verbose"),
          options.getBoolean("syslog"),
          (File) options.get("logconfig"),
          options.getBoolean("no_log_setup"));

      final String uriString = options.getString("docker");
      try {
        new URI(uriString);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException("Bad docker endpoint: " + uriString, e);
      }

      final String name = options.getString("name");

      agentConfig = new AgentConfig()
          .setName(name)
          .setZooKeeperConnectionString(options.getString("zk"))
          .setSite(options.getString("site"))
          .setMuninReporterPort(options.getInt("munin_port"))
          .setDockerEndpoint(options.getString("docker"));

    } catch (ArgumentParserException e) {
      parser.handleError(e);
      throw e;
    }
  }

  public static void addArgs(final ArgumentParser parser) {

    parser.addArgument("--name")
        .setDefault(getHostName())
        .help("agent name");

    parser.addArgument("-s", "--site")
        .setDefault(Defaults.SITES.get(0))
        .help("backend site");

    parser.addArgument("--zk")
        .setDefault("localhost:2181")
        .help("zookeeper connection string");

    parser.addArgument("--munin-port")
        .type(Integer.class)
        .setDefault(4952)
        .help("munin port (0 = disabled)");

    parser.addArgument("--docker")
        .setDefault("http://localhost:4160")
        .help("docker endpoint");

    parser.addArgument("-v", "--verbose")
        .action(Arguments.count());

    parser.addArgument("--syslog")
        .help("Log to syslog.")
        .action(storeTrue());

    parser.addArgument("--logconfig")
        .type(fileType().verifyExists().verifyCanRead())
        .help("Logback configuration file.");

    parser.addArgument("--no-log-setup")
        .action(storeTrue())
        .help(SUPPRESS);
  }

  private static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw propagate(e);
    }
  }

  public Namespace getNamespace() {
    return options;
  }

  public LoggingConfig getLoggingConfig() {
    return loggingConfig;
  }

  public AgentConfig getAgentConfig() {
    return agentConfig;
  }

}

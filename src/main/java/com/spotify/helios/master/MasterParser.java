/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.master;

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
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;

import static net.sourceforge.argparse4j.impl.Arguments.*;

public class MasterParser {

  private final Namespace options;
  private final LoggingConfig loggingConfig;
  private final MasterConfig masterConfig;

  public MasterParser(final String... args) throws ArgumentParserException, JSONException, IOException {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser("sphelios-master")
        .defaultHelp(true)
        .description("Spotify Helios Master");

    addArgs(parser);

    try {
      this.options = parser.parseArgs(args);

      this.loggingConfig = new LoggingConfig(options.getInt("verbose"),
                                             options.getBoolean("syslog"),
                                             (File) options.get("logconfig"),
                                             options.getBoolean("no_log_setup"));

      final String bindHttp = options.getString("http");
      final InetSocketAddress bindHttpAddress = parseSocketAddress(bindHttp);

      this.masterConfig = new MasterConfig()
          .setHermesEndpoint(options.getString("hm"))
          .setHttpEndpoint(bindHttpAddress)
          .setZooKeeperConnectString(options.getString("zk"))
          .setSite(options.getString("site"))
          .setMuninReporterPort(options.getInt("munin_port"));

    } catch (ArgumentParserException e) {
      parser.handleError(e);
      throw e;
    }
  }

  public static void addArgs(final ArgumentParser parser) {

    parser.addArgument("-s", "--site")
        .setDefault(Defaults.SITES.get(0))
        .help("backend site");

    parser.addArgument("--hm")
        .setDefault(Defaults.MASTER_HM_BIND)
        .help("hermes endpoint");

    parser.addArgument("--http")
        .setDefault(Defaults.MASTER_HTTP_BIND)
        .help("http endpoint");

    parser.addArgument("--zk")
        .setDefault("localhost:2181")
        .help("zookeeper connection string");

    parser.addArgument("--munin-port")
        .type(Integer.class)
        .setDefault(4951)
        .help("munin port (0 = disabled)");

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

  private InetSocketAddress parseSocketAddress(final String addressString) {
    final InetSocketAddress address;
    try {
      final URI u = new URI("http://" + addressString);
      address = new InetSocketAddress(u.getHost(), u.getPort());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Bad address: " + addressString, e);
    }
    return address;
  }

  public Namespace getNamespace() {
    return options;
  }

  public LoggingConfig getLoggingConfig() {
    return loggingConfig;
  }

  public MasterConfig getMasterConfig() {
    return masterConfig;
  }

}

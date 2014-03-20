package com.spotify.helios.servicescommon;

import com.google.common.io.CharStreams;

import com.spotify.helios.common.LoggingConfig;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
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

import static com.google.common.base.Throwables.propagate;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.sourceforge.argparse4j.impl.Arguments.SUPPRESS;
import static net.sourceforge.argparse4j.impl.Arguments.fileType;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class ServiceParser {

  private final Namespace options;
  private final LoggingConfig loggingConfig;
  private final File serviceRegistrarPlugin;
  private final String getServiceRegistryAddress;

  public ServiceParser(final String programName, final String description, String... args)
      throws ArgumentParserException {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser(programName)
        .defaultHelp(true)
        .description(description);

    parser.addArgument("--name")
        .setDefault(getHostName())
        .help("hostname to register as");

    parser.addArgument("-s", "--site")
        .help("backend site");

    parser.addArgument("--service-registry")
        .type(String.class)
        .help("Service registry address.");

    parser.addArgument("--service-registrar-plugin")
        .type(fileType().verifyExists().verifyCanRead())
        .help("Service registration plugin.");

    parser.addArgument("--zk")
        .setDefault("localhost:2181")
        .help("zookeeper connection string");

    parser.addArgument("--zk-session-timeout")
        .type(Integer.class)
        .setDefault((int) SECONDS.toMillis(60))
        .help("zookeeper session timeout");

    parser.addArgument("--zk-connection-timeout")
        .type(Integer.class)
        .setDefault((int) SECONDS.toMillis(15))
        .help("zookeeper connection timeout");

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

    addArgs(parser);

    try {
      this.options = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      throw e;
    }

    this.loggingConfig = new LoggingConfig(options.getInt("verbose"),
                                           options.getBoolean("syslog"),
                                           (File) options.get("logconfig"),
                                           options.getBoolean("no_log_setup"));

    this.serviceRegistrarPlugin = (File) options.get("service_registrar_plugin");

    this.getServiceRegistryAddress = options.getString("service_registry");
  }

  protected void addArgs(final ArgumentParser parser) {
  }

  public Namespace getNamespace() {
    return options;
  }

  public LoggingConfig getLoggingConfig() {
    return loggingConfig;
  }

  public String getServiceRegistryAddress() {
    return getServiceRegistryAddress;
  }

  public Path getServiceRegistrarPlugin() {
    return serviceRegistrarPlugin != null ? serviceRegistrarPlugin.toPath() : null;
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

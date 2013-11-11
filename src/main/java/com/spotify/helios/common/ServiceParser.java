package com.spotify.helios.common;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;

import static net.sourceforge.argparse4j.impl.Arguments.SUPPRESS;
import static net.sourceforge.argparse4j.impl.Arguments.fileType;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class ServiceParser {

  private final Namespace options;
  private final LoggingConfig loggingConfig;

  public ServiceParser(final String programName, final String description, String... args)
      throws ArgumentParserException {

    final ArgumentParser parser = ArgumentParsers.newArgumentParser(programName)
        .defaultHelp(true)
        .description(description);

    parser.addArgument("-s", "--site")
        .setDefault(Defaults.SITES.get(0))
        .help("backend site");

    parser.addArgument("--zk")
        .setDefault("localhost:2181")
        .help("zookeeper connection string");

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
  }

  protected void addArgs(final ArgumentParser parser) {
  }

  public Namespace getNamespace() {
    return options;
  }

  public LoggingConfig getLoggingConfig() {
    return loggingConfig;
  }

}

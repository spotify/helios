/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.cli;

import com.spotify.helios.common.LoggingConfig;
import com.spotify.logging.LoggingConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;

import static com.google.common.collect.Iterables.get;
import static com.spotify.logging.LoggingConfigurator.Level.*;
import static java.util.Arrays.asList;

/**
 * Instantiates and runs helios CLI.
 */
public class CliMain {

  private static final Logger log = LoggerFactory.getLogger(CliMain.class);

  private final CliParser parser;

  public static void main(final String... args) {
    try {
      int exitCode = new CliMain(args).run();
      System.exit(exitCode);
    } catch (Throwable e) {
      //log.error("Uncaught exception", e);
      System.exit(1);
    }

    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }

  public CliMain(final CliParser parser) {
    this.parser = parser;
    setupLogging();
  }

  public CliMain(final PrintStream out, final PrintStream err, final String... args) throws Exception {
    this(CliParser.createDefaultParser(out, args));
  }

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public CliMain(final String... args) throws Exception {
    this(System.out, System.err, args);
  }

  public int run() {
    try {
      return parser.getCommand().runControl(parser.getNamespace());
    } catch (Exception e) {
      log.error("command failed", e);
      return 1;
    }
  }

  private void setupLogging() {
    final LoggingConfig config = parser.getLoggingConfig();

    if (config.getNoLogSetup()) {
      return;
    }

    final int verbose = config.getVerbosity();
    final LoggingConfigurator.Level level = get(asList(INFO, DEBUG, ALL), verbose, ALL);
    final File logconfig = config.getConfigFile();

    if (logconfig != null) {
      LoggingConfigurator.configure(logconfig);
    } else {
      if (config.isSyslog()) {
        LoggingConfigurator.configureSyslogDefaults("helios", level);
      } else {
        LoggingConfigurator.configureDefaults("helios", level);
      }
    }
  }

}

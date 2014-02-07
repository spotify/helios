/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.cli;

import com.spotify.helios.common.LoggingConfig;
import com.spotify.logging.LoggingConfigurator;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;

import static com.google.common.collect.Iterables.get;
import static com.spotify.logging.LoggingConfigurator.Level.ALL;
import static com.spotify.logging.LoggingConfigurator.Level.DEBUG;
import static com.spotify.logging.LoggingConfigurator.Level.INFO;
import static java.util.Arrays.asList;

/**
 * Instantiates and runs helios CLI.
 */
public class CliMain {

  private static final Logger log = LoggerFactory.getLogger(CliMain.class);

  private final CliParser parser;
  private final PrintStream out;
  private final PrintStream err;

  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  public static void main(final String... args) {
    try {
      int exitCode = new CliMain(System.out, System.err, args).run();
      System.exit(exitCode);
    } catch (Throwable e) {
      // don't print error message for arg parser exception, because parser will do that
      if (!(e instanceof ArgumentParserException)) {
        System.err.println(e.getMessage());
      }
      System.exit(1);
    }
  }

  public CliMain(final PrintStream out, final PrintStream err, final String... args)
      throws Exception {
    this.parser = new CliParser(args);
    this.out = out;
    this.err = err;
    setupLogging();
  }

  public int run() {
    try {
      return parser.getCommand().run(parser.getNamespace(), parser.getTargets(), out, err,
                                     parser.getUsername(), parser.getJson());
    } catch (Exception e) {
      // print entire stack trace in verbose mode, otherwise just the exception message
      if (parser.getNamespace().getInt("verbose") > 0) {
        e.printStackTrace(err);
      } else {
        err.println(e.getMessage());
      }
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

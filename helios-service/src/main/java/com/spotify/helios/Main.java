/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios;

import com.spotify.helios.cli.LoggingConfig;
import com.spotify.helios.cli.Parser;
import com.spotify.logging.LoggingConfigurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.util.concurrent.CountDownLatch;

import static com.google.common.collect.Iterables.get;
import static com.spotify.logging.LoggingConfigurator.Level;
import static com.spotify.logging.LoggingConfigurator.Level.ALL;
import static com.spotify.logging.LoggingConfigurator.Level.DEBUG;
import static com.spotify.logging.LoggingConfigurator.Level.INFO;
import static java.util.Arrays.asList;

/**
 * Instantiates and runs helios.
 */
public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  private final CountDownLatch runLatch = new CountDownLatch(1);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);

  private final Parser parser;

  public static void main(final String... args) {
    try {
      int exitCode = new Main(args).run();
      System.exit(exitCode);
    } catch (Throwable e) {
      //log.error("Uncaught exception", e);
      System.exit(1);
    }

    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }

  public Main(final Parser parser) {
    this.parser = parser;

    setupLogging();
  }

  public Main(final PrintStream out, final PrintStream err, final String... args) throws Exception {
    this(Parser.createDefaultParser(out, args));
  }

  public Main(final String... args) throws Exception {
    this(System.out, System.err, args);
  }

  public int run() {
    try {
      final Entrypoint entrypoint = parser.getEntrypoint();
      return entrypoint.enter(runLatch);
    } catch (Exception e) {
      log.error("command failed", e);
      return 1;
    } finally {
      shutdownLatch.countDown();
    }
  }

  private void setupLogging() {
    final LoggingConfig config = parser.getLoggingConfig();

    if (config.getNoLogSetup()) {
      return;
    }

    final int verbose = config.getVerbosity();
    final Level level = get(asList(INFO, DEBUG, ALL), verbose, ALL);
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

  public void shutdown() {
    runLatch.countDown();
  }

  public void join() throws InterruptedException {
    shutdownLatch.await();
  }
}

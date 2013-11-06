/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.common.Main;
import com.spotify.logging.LoggingConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import static com.google.common.collect.Iterables.get;
import static com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly;
import static com.spotify.logging.LoggingConfigurator.Level.*;
import static java.util.Arrays.asList;

/**
 * Instantiates and runs helios.
 */
public class MasterMain implements Main {

  private static final Logger log = LoggerFactory.getLogger(MasterMain.class);

  private final CountDownLatch runLatch = new CountDownLatch(1);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final MasterConfig masterConfig;

  public static void main(final String... args) {
    try {
      int exitCode = new MasterMain(args).run();
      System.exit(exitCode);
    } catch (Throwable e) {
      //log.error("Uncaught exception", e);
      System.exit(1);
    }

    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }

  public MasterMain(final String... args) throws Exception {
    MasterParser parser = new MasterParser(args);
    setupLogging(parser.getLoggingConfig());
    masterConfig = parser.getMasterConfig();
  }

  public int run() {
    try {
      final MasterService service = new MasterService(masterConfig);
      service.start();
      awaitUninterruptibly(runLatch);
      service.stop();
      return 0;
    } catch (Exception e) {
      log.error("command failed", e);
      return 1;
    } finally {
      shutdownLatch.countDown();
    }
  }

  private void setupLogging(LoggingConfig config) {
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

  public void shutdown() {
    runLatch.countDown();
  }

  public void join() throws InterruptedException {
    shutdownLatch.await();
  }

}

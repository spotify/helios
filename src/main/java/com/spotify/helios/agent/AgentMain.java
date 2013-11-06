/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.agent;

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
public class AgentMain implements Main {

  private static final Logger log = LoggerFactory.getLogger(AgentMain.class);

  private final CountDownLatch runLatch = new CountDownLatch(1);
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AgentConfig agentConfig;

  public static void main(final String... args) {
    try {
      int exitCode = new AgentMain(args).run();
      System.exit(exitCode);
    } catch (Throwable e) {
      //log.error("Uncaught exception", e);
      System.exit(1);
    }

    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }

  public AgentMain(final String... args) throws Exception {
    AgentParser parser = new AgentParser(args);
    setupLogging(parser.getLoggingConfig());
    agentConfig = parser.getAgentConfig();
  }

  public int run() {
    try {
      final AgentService service = new AgentService(agentConfig);
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

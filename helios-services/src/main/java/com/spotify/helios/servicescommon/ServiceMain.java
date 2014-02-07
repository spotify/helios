package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.AbstractIdleService;

import com.spotify.helios.common.LoggingConfig;
import com.spotify.logging.LoggingConfigurator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.google.common.collect.Iterables.get;
import static com.spotify.logging.LoggingConfigurator.Level.ALL;
import static com.spotify.logging.LoggingConfigurator.Level.DEBUG;
import static com.spotify.logging.LoggingConfigurator.Level.INFO;
import static java.util.Arrays.asList;

public abstract class ServiceMain extends AbstractIdleService {

  protected final Logger log = LoggerFactory.getLogger(this.getClass());

  protected ServiceMain(LoggingConfig loggingConfig) {
    setupLogging(loggingConfig);
  }

  protected void setupLogging(LoggingConfig config) {
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
      String sentryDsn = System.getenv("SENTRY_DSN");
      if (sentryDsn != null) {
        LoggingConfigurator.addSentryAppender(sentryDsn);
      }
    }
  }
}

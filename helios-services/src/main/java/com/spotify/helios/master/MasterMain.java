/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.servicescommon.ServiceMain;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.json.ObjectMapperFactory;
import com.yammer.dropwizard.validation.Validator;

import net.sourceforge.argparse4j.inf.ArgumentParserException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Instantiates and runs the helios master. We do our own bootstrapping instead of using
 * {@link com.yammer.dropwizard.config.Bootstrap} because we want more control over logging etc.
 */
public class MasterMain extends ServiceMain {

  private static final Logger log = LoggerFactory.getLogger(MasterMain.class);

  private final MasterConfig masterConfig;
  private MasterService service;

  public MasterMain(final String[] args) throws ArgumentParserException {
    this(new MasterParser(args));
  }

  public MasterMain(final MasterParser parser) {
    this(parser.getMasterConfig(), parser.getLoggingConfig());
  }

  public MasterMain(final MasterConfig masterConfig, final LoggingConfig loggingConfig) {
    super(loggingConfig, masterConfig.getSentryDsn());
    this.masterConfig = masterConfig;
  }

  @Override
  protected void startUp() throws Exception {
    final Environment environment = new Environment("helios-master", masterConfig,
                                                    new ObjectMapperFactory(), new Validator());
    service = new MasterService(masterConfig, environment);
    service.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    service.stopAsync().awaitTerminated();
  }

  public static void main(final String... args) {
    try {
      final MasterMain main = new MasterMain(args);
      main.startAsync().awaitRunning();
      main.awaitTerminated();
    } catch (Throwable e) {
      log.error("Uncaught exception", e);
      System.exit(1);
    }
    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }
}

/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.master;

import com.spotify.helios.common.LoggingConfig;
import com.spotify.helios.common.ServiceMain;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

/**
 * Instantiates and runs helios master.
 */
public class MasterMain extends ServiceMain {

  private final MasterConfig masterConfig;
  private MasterService service;

  public MasterMain(final String[] args) throws ArgumentParserException {
    this(new MasterParser(args));
  }

  public MasterMain(final MasterParser parser) {
    this(parser.getMasterConfig(), parser.getLoggingConfig());
  }

  public MasterMain(final MasterConfig masterConfig, final LoggingConfig loggingConfig) {
    super(loggingConfig);
    this.masterConfig = masterConfig;
  }

  @Override
  protected void startUp() throws Exception {
    service = new MasterService(masterConfig);
    service.start();
  }

  @Override
  protected void shutDown() throws Exception {
    service.stop();
  }

  public static void main(final String[] args) {
    try {
      final MasterMain main = new MasterMain(args);
      main.startAsync();
      main.awaitTerminated();
    } catch (Throwable e) {
      System.exit(1);
    }
    // Ensure we exit even if there's lingering non-daemon threads
    System.exit(0);
  }
}

/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.cli;

import java.io.File;

public class LoggingConfig {

  private final int verbosity;
  private final boolean syslog;
  private final File configFile;
  private final Boolean noLogSetup;

  public LoggingConfig(final int verbosity, final boolean syslog, final File configFile,
                       final Boolean noLogSetup) {
    this.verbosity = verbosity;
    this.syslog = syslog;
    this.configFile = configFile;
    this.noLogSetup = noLogSetup;
  }

  public int getVerbosity() {
    return verbosity;
  }

  public boolean isSyslog() {
    return syslog;
  }

  public File getConfigFile() {
    return configFile;
  }

  public Boolean getNoLogSetup() {
    return noLogSetup;
  }
}

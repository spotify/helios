/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.common;

import java.io.File;

public class LoggingConfig {

  private final int verbosity;
  private final boolean syslog;
  private final File configFile;
  private final boolean noLogSetup;

  public LoggingConfig(final int verbosity, final boolean syslog, final File configFile,
                       final boolean noLogSetup) {
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

  public boolean getNoLogSetup() {
    return noLogSetup;
  }
}

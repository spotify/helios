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

//* Using the meanings from http://semver.org
public class VersionCompatibility {
  public static final String HELIOS_VERSION_HEADER = "Helios-Version";
  public static final String HELIOS_SERVER_VERSION_HEADER = "Helios-Server-Version";
  public static final String HELIOS_VERSION_STATUS_HEADER = "Helios-Version-Status";

  public enum Status {
    EQUAL,
    COMPATIBLE,
    MAYBE,
    INCOMPATIBLE,
    UPGRADE_SOON,
    INVALID,
    MISSING
  }

  public static Status getStatus(final PomVersion serverVersion, final PomVersion clientVersion) {
    // WARN has to be dealt with on an ad-hoc basis.  Since no warnings at this time, we don't
    // have any code here

    if (serverVersion.equals(clientVersion)) {
      return Status.EQUAL;
    }

    if (serverVersion.getMajor() != clientVersion.getMajor()) {
      return Status.INCOMPATIBLE;
    }

    // since pre-1.0.0, if its a patch level behind, warn -- cheesyish for now
    // this rule will change over time....
    if ((serverVersion.getMajor() == clientVersion.getMajor())
        && (serverVersion.getMinor() == clientVersion.getMinor())
        && ((serverVersion.getPatch() - 1) >= clientVersion.getPatch())) {
      return Status.UPGRADE_SOON;
    }

    // older clients, newer server within major version
    if (serverVersion.getMinor() >= clientVersion.getMinor()) {
      return Status.COMPATIBLE;
    }

    // newer client, older server within major version
    return Status.MAYBE;
  }
}

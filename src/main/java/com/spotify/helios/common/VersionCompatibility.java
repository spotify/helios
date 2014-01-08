package com.spotify.helios.common;

import com.spotify.helios.master.PomVersion;

//* Using the meanings from http://semver.org
public class VersionCompatibility {
  public enum Status {
    EQUAL,
    COMPATIBLE,
    MAYBE,
    INCOMPATIBLE,
    WARN
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

    // older clients, newer server within major version
    if (serverVersion.getMinor() >= clientVersion.getMinor()) {
      return Status.COMPATIBLE;
    }

    // newer client, older server within major version
    return Status.MAYBE;
  }
}
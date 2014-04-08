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

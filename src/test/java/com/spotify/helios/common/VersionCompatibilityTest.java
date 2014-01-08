package com.spotify.helios.common;

import com.spotify.helios.master.PomVersion;

import org.junit.Test;

import static org.junit.Assert.*;
import static com.spotify.helios.common.VersionCompatibility.Status;

public class VersionCompatibilityTest {

  @Test
  public void test() {
    PomVersion server = PomVersion.parse("1.3.9");
    assertEquals(Status.EQUAL, VersionCompatibility.getStatus(server, server));
    assertEquals(Status.COMPATIBLE, VersionCompatibility.getStatus(
        server, PomVersion.parse("1.3.8")));
    assertEquals(Status.COMPATIBLE, VersionCompatibility.getStatus(
        server, PomVersion.parse("1.2.8")));
    assertEquals(Status.MAYBE, VersionCompatibility.getStatus(
        server, PomVersion.parse("1.4.8")));
    assertEquals(Status.INCOMPATIBLE, VersionCompatibility.getStatus(
        server, PomVersion.parse("9.0.0")));
  }
}

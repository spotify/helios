package com.spotify.helios.master;

import com.spotify.helios.TestBase;

import org.junit.Test;

import static org.junit.Assert.*;

public class PomVersionTest extends TestBase {

  @Test
  public void testNormal() {
    PomVersion v = PomVersion.parse("1.2.9");
    assertEquals(9, v.getPatch());
    assertEquals(2, v.getMinor());
    assertEquals(1, v.getMajor());
    assertFalse(v.isSnapshot());
    assertEquals("1.2.9", v.toString());

    v = PomVersion.parse("8.6.5-SNAPSHOT");
    assertEquals(5, v.getPatch());
    assertEquals(6, v.getMinor());
    assertEquals(8, v.getMajor());
    assertTrue(v.isSnapshot());
    assertEquals("8.6.5-SNAPSHOT", v.toString());
  }

  @Test
  public void testFailures() {
    try {
      PomVersion.parse("1.2");
      fail("should throw");
    } catch (RuntimeException e) { // ok
      assertTrue("should contain something about format", e.getMessage().contains("format"));
    }

    try {
      PomVersion.parse("1.2.3.4");
      fail("should throw");
    } catch (RuntimeException e) { // ok
      assertTrue("should contain something about format", e.getMessage().contains("format"));
    }

    try {
      PomVersion.parse("x.y.z");
      fail("should throw");
    } catch (RuntimeException e) { // ok
      assertTrue("should contain something about must be numbers",
          e.getMessage().contains("number"));
    }
  }
}

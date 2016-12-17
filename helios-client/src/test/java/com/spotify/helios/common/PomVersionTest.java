/*-
 * -\-\-
 * Helios Client
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PomVersionTest {

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

  @Test
  public void testComparable() {
    final PomVersion v1 = new PomVersion(false, 1, 0, 5);
    final PomVersion v2 = new PomVersion(false, 2, 1, 0);

    assertTrue(v1.compareTo(v2) < 0);
    assertTrue(v2.compareTo(v1) > 0);
  }

  @Test
  public void testComparable_Equals() {
    final PomVersion v1 = new PomVersion(false, 1, 0, 0);
    final PomVersion v2 = new PomVersion(false, 1, 0, 0);
    assertEquals(0, v1.compareTo(v2));
    assertEquals(0, v2.compareTo(v1));
  }
}

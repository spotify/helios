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

package com.spotify.helios.common.descriptors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.spotify.helios.common.Json;
import org.junit.Test;

public class HostSelectorTest {

  @Test
  public void testParseEquals() {
    assertEquals(new HostSelector("A", HostSelector.Operator.EQUALS, "B"),
        HostSelector.parse("A=B"));
    assertEquals(new HostSelector("A", HostSelector.Operator.EQUALS, "B"),
        HostSelector.parse("A = B"));
    assertEquals(new HostSelector("A", HostSelector.Operator.EQUALS, "B"),
        HostSelector.parse("A =B"));
    assertEquals(new HostSelector("A", HostSelector.Operator.EQUALS, "B"),
        HostSelector.parse("A= B"));
    assertEquals(new HostSelector("A", HostSelector.Operator.EQUALS, "B"),
        HostSelector.parse("A\t\t=  B"));
  }

  @Test
  public void testParseNotEquals() {
    assertEquals(new HostSelector("A", HostSelector.Operator.NOT_EQUALS, "B"),
        HostSelector.parse("A!=B"));
    assertEquals(new HostSelector("A", HostSelector.Operator.NOT_EQUALS, "B"),
        HostSelector.parse("A != B"));
    assertEquals(new HostSelector("A", HostSelector.Operator.NOT_EQUALS, "B"),
        HostSelector.parse("A !=B"));
    assertEquals(new HostSelector("A", HostSelector.Operator.NOT_EQUALS, "B"),
        HostSelector.parse("A!= B"));
    assertEquals(new HostSelector("A", HostSelector.Operator.NOT_EQUALS, "B"),
        HostSelector.parse("A\t\t!=  B"));
  }

  @Test
  public void testParseAllowedCharacters() {
    assertEquals(new HostSelector("foo", HostSelector.Operator.EQUALS, "123"),
        HostSelector.parse("foo=123"));
    assertEquals(new HostSelector("_abc", HostSelector.Operator.NOT_EQUALS, "d-e"),
        HostSelector.parse("_abc!=d-e"));
  }

  @Test
  public void testParseDisallowedCharacters() {
    assertNull(HostSelector.parse("foo = @123"));
    assertNull(HostSelector.parse("f/oo = 123"));
    // Verify equal not allowed in label and operand
    assertNull(HostSelector.parse("f=oo = 123"));
    assertNull(HostSelector.parse("foo = 12=3"));
    // Verify spaces not allowed in label and operand
    assertNull(HostSelector.parse("fo o = 123"));
    assertNull(HostSelector.parse("foo = 1 23"));
    // Verify ! not allowed in label and operand
    assertNull(HostSelector.parse("foo=!123"));
    assertNull(HostSelector.parse("!foo=bar"));
    // Verify fails on unknown operators
    assertNull(HostSelector.parse("foo or 123"));
    assertNull(HostSelector.parse("foo==123"));
    assertNull(HostSelector.parse("foo&&123"));
    // Verify fails on empty label or operand
    assertNull(HostSelector.parse("=123"));
    assertNull(HostSelector.parse(" =123"));
    assertNull(HostSelector.parse(" = 123"));
    assertNull(HostSelector.parse("foo="));
    assertNull(HostSelector.parse("foo= "));
    assertNull(HostSelector.parse("foo = "));
  }

  @Test
  public void testEqualsMatch() {
    final HostSelector hostSelector = HostSelector.parse("A=B");
    assertTrue(hostSelector.matches("B"));
    assertFalse(hostSelector.matches("Bb"));
    assertFalse(hostSelector.matches("b"));
    assertFalse(hostSelector.matches("A"));
  }

  @Test
  public void testNotEqualsMatch() {
    final HostSelector hostSelector = HostSelector.parse("A!=B");
    assertFalse(hostSelector.matches("B"));
    assertTrue(hostSelector.matches("Bb"));
    assertTrue(hostSelector.matches("b"));
    assertTrue(hostSelector.matches("A"));
  }

  @Test
  public void testInOperator() {
    final HostSelector hostSelector = HostSelector.parse("a in (foo, bar)");
    assertTrue(hostSelector.matches("foo"));
    assertTrue(hostSelector.matches("bar"));
    assertFalse(hostSelector.matches("baz"));

    final HostSelector hostSelector2 = HostSelector.parse("a in(foo,bar)");
    assertTrue(hostSelector2.matches("foo"));
    assertTrue(hostSelector2.matches("bar"));
    assertFalse(hostSelector2.matches("baz"));

    final HostSelector hostSelector3 = HostSelector.parse("a in(foo)");
    assertTrue(hostSelector3.matches("foo"));
    assertFalse(hostSelector3.matches("baz"));
  }

  @Test
  public void testInOperatorEquality() {
    assertEquals(
        HostSelector.parse("a in (foo,bar)"),
        HostSelector.parse("a in (foo, bar)")
    );

    assertEquals(
        HostSelector.parse("a in (foo,bar)"),
        HostSelector.parse("a in (bar,foo)")
    );

    assertEquals(
        HostSelector.parse("a in (foo)"),
        HostSelector.parse("a in (foo,foo)")
    );
  }

  @Test
  public void testInOperatorSerialization() throws Exception {
    final HostSelector orig = HostSelector.parse("a in (foo,bar)");
    final HostSelector parsed = Json.read(Json.asString(orig), HostSelector.class);
    assertEquals(orig, parsed);
  }

  @Test
  public void testInOperatorEmptySet() {
    final HostSelector hostSelector = HostSelector.parse("a in ()");
    assertFalse(hostSelector.matches("foo"));
    assertFalse(hostSelector.matches("bar"));
    assertFalse(hostSelector.matches("baz"));

    final HostSelector hostSelector2 = HostSelector.parse("a in()");
    assertFalse(hostSelector2.matches("foo"));
    assertFalse(hostSelector2.matches("bar"));
    assertFalse(hostSelector2.matches("baz"));
  }

  @Test
  public void testNotInOperator() {
    final HostSelector hostSelector = HostSelector.parse("a notin (foo, bar)");
    assertFalse(hostSelector.matches("foo"));
    assertFalse(hostSelector.matches("bar"));
    assertTrue(hostSelector.matches("baz"));

    final HostSelector hostSelector2 = HostSelector.parse("a notin(foo,bar)");
    assertFalse(hostSelector2.matches("foo"));
    assertFalse(hostSelector2.matches("bar"));
    assertTrue(hostSelector2.matches("baz"));

    final HostSelector hostSelector3 = HostSelector.parse("a notin(foo)");
    assertFalse(hostSelector3.matches("foo"));
    assertTrue(hostSelector3.matches("baz"));
  }

  @Test
  public void testNotInOperatorEquality() {
    assertEquals(
        HostSelector.parse("a notin (foo,bar)"),
        HostSelector.parse("a notin (foo, bar)")
    );

    assertEquals(
        HostSelector.parse("a notin (foo,bar)"),
        HostSelector.parse("a notin (bar,foo)")
    );

    assertEquals(
        HostSelector.parse("a notin (foo)"),
        HostSelector.parse("a notin (foo,foo)")
    );
  }

  @Test
  public void testNotInOperatorSerialization() throws Exception {
    final HostSelector orig = HostSelector.parse("a notin (foo,bar)");
    final HostSelector parsed = Json.read(Json.asString(orig), HostSelector.class);
    assertEquals(orig, parsed);
  }

  @Test
  public void testNotInOperatorEmptySet() {
    final HostSelector hostSelector = HostSelector.parse("a notin ()");
    assertTrue(hostSelector.matches("foo"));
    assertTrue(hostSelector.matches("bar"));
    assertTrue(hostSelector.matches("baz"));

    final HostSelector hostSelector2 = HostSelector.parse("a notin()");
    assertTrue(hostSelector2.matches("foo"));
    assertTrue(hostSelector2.matches("bar"));
    assertTrue(hostSelector2.matches("baz"));
  }

  @Test
  public void testToPrettyString() {
    assertEquals("A != B", HostSelector.parse("A!=B").toPrettyString());
    assertEquals("A = B", HostSelector.parse("A=B").toPrettyString());
  }
}

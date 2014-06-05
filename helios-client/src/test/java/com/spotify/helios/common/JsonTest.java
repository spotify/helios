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

import org.junit.Before;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class JsonTest {

  static final String EXPECTED_JSON =
      "{\"c\":\"bar\",\"foo\":{\"a\":\"hello\",\"b\":17,\"c\":{\"1\":1,\"2\":\"two\"}}}";

  MessageDigest sha1;
  byte[] expectedDigest;

  @Before
  public void setup() throws Exception {
    sha1 = MessageDigest.getInstance("SHA-1");
    expectedDigest = sha1.digest(EXPECTED_JSON.getBytes());
  }

  public static class Foo {

    public int b;
    public String a;
    public Map<String, Object> c;
    public String ignoredNullString;
    public Map<String, Object> ignoredNullMap;
    public List<String> ignoredNullList;
    public String ignoredEmptyString;
    public Map<String, Object> ignoredEmptyMap;
    public List<String> ignoredEmptyList;
  }

  public static class Bar {

    public String c;
    public Foo foo;
  }

  @Test
  public void testObjectSha1() throws Exception {
    final Bar bar = new Bar() {{
      c = "bar";
      foo = new Foo() {{
        b = 17;
        a = "hello";
        c = new LinkedHashMap<String, Object>() {{
          put("2", "two");
          put("1", 1);
        }};
        ignoredNullString = null;
        ignoredNullMap = null;
        ignoredNullList = null;
        ignoredEmptyString = "";
        ignoredEmptyMap = Collections.emptyMap();
        ignoredEmptyList = Collections.emptyList();
      }};
    }};
    final String barJson = Json.asNormalizedString(bar);
    assertEquals(EXPECTED_JSON, barJson);
    final byte[] digest = Json.sha1digest(bar);
    assertArrayEquals(expectedDigest, digest);
  }

  public static class SomePojo {
    public String foo;
  }

  @Test
  public void verifyIgnoresUnknownFields() throws Exception {
    final SomePojo somePojo = Json.read("{\"foo\":\"1\", \"bar\":\"2\"}", SomePojo.class);
    assertEquals("1", somePojo.foo);
  }

  @Test
  public void verifyPrettyOutput() {
    final String json = Json.asPrettyStringUnchecked(new SomePojo() {{foo = "bar";}});
    assertEquals("{\n" +
                 "  \"foo\" : \"bar\"\n" +
                 "}", json);
  }
}

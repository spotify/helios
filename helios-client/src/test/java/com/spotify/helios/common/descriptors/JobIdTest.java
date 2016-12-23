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

import com.spotify.helios.common.Json;
import java.io.IOException;
import org.junit.Test;

public class JobIdTest {

  @Test
  public void testFullToString() {
    final JobId id = JobId.newBuilder().setName("foo").setVersion("bar").setHash("baz").build();
    assertEquals("foo:bar:baz", id.toString());
  }

  @Test
  public void testShortToString() {
    final JobId id = JobId.newBuilder().setName("foo").setVersion("bar").build();
    assertEquals("foo:bar", id.toString());
  }

  @Test
  public void testJsonParsing() throws IOException {
    final String json = "\"foo:17:deadbeef\"";
    final JobId jobId = Json.read(json, JobId.class);

    final JobId expectedJobId = JobId.newBuilder()
        .setName("foo")
        .setVersion("17")
        .setHash("deadbeef")
        .build();

    assertEquals(expectedJobId, jobId);
  }

  @Test
  public void testJsonSerialization() throws IOException {
    final String expectedJson = "\"foo:17:deadbeef\"";

    final JobId jobId = JobId.newBuilder()
        .setName("foo")
        .setVersion("17")
        .setHash("deadbeef")
        .build();

    final String json = Json.asStringUnchecked(jobId);
    assertEquals(expectedJson, json);
  }
}

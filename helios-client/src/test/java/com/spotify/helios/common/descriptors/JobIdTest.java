/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.spotify.helios.TestBase;
import com.spotify.helios.common.Json;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class JobIdTest extends TestBase {

  public void testFullToString() {
    final JobId id = JobId.newBuilder().setName("foo").setVersion("bar").setHash("baz").build();
    assertEquals("foo:bar:baz", id.toString());
  }

  public void testShortToString() {
    final JobId id = JobId.newBuilder().setName("foo").setVersion("bar").build();
    assertEquals("foo:bar", id.toString());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColonInNameNotAllowed() {
    JobId.newBuilder().setName("foo:bar").setVersion("17").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColonInVersionNotAllowed() {
    JobId.newBuilder().setName("foo").setVersion("release:17").build();
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

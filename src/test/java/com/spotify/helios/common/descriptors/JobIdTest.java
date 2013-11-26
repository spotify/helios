/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.descriptors;

import com.spotify.helios.common.Json;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class JobIdTest {
  @Test
  public void testJsonParsing() throws IOException {
    final String json = "\"foo:17:deadbeef\"";
    final JobId jobId = Json.read(json, JobId.class);

    final JobId expectedJobId = JobId.newBuilder()
        .setName("foo")
        .setVersion("17")
        .setHash("deadbeef")
        .build();

    Assert.assertEquals(expectedJobId, jobId);
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
    Assert.assertEquals(expectedJson, json);
  }
}

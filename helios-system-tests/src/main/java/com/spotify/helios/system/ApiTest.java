/*-
 * -\-\-
 * Helios System Tests
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

package com.spotify.helios.system;

import static com.google.common.io.ByteStreams.toByteArray;
import static com.spotify.helios.common.protocol.CreateJobResponse.Status.OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.protocol.CreateJobResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import org.junit.Test;

public class ApiTest extends SystemTestBase {

  /**
   * Verify that the Helios master generates and returns a hash if the submitted job creation
   * request does not include one.
   */
  @Test
  public void testHashLessJobCreation() throws Exception {
    startDefaultMaster();

    final Job job = Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setCommand(IDLE_COMMAND)
        .setCreatingUser(TEST_USER)
        .build();

    // Remove the hash from the id in the json serialized job
    final ObjectNode json = (ObjectNode) Json.reader().readTree(Json.asString(job));
    json.set("id", TextNode.valueOf(testJobName + ":" + testJobVersion));

    final HttpURLConnection req = post("/jobs?user=" + TEST_USER,
        Json.asBytes(json));

    assertEquals(req.getResponseCode(), 200);

    final CreateJobResponse res = Json.read(toByteArray(req.getInputStream()),
        CreateJobResponse.class);

    assertEquals(OK, res.getStatus());
    assertTrue(res.getErrors().isEmpty());
    assertEquals(job.getId().toString(), res.getId());
  }

  private HttpURLConnection post(final String path, final byte[] body) throws IOException {
    final URL url = new URL(masterEndpoint() + path);
    final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoInput(true);
    connection.setDoOutput(true);
    connection.getOutputStream().write(body);
    return connection;
  }
}

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

package com.spotify.helios.system;

import com.google.common.collect.Maps;

import com.spotify.helios.Polling;
import com.spotify.helios.common.PomVersion;
import com.spotify.helios.common.Version;
import com.spotify.helios.common.VersionCompatibility;
import com.spotify.helios.Parallelized;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.spotify.helios.common.Version.POM_VERSION;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_SERVER_VERSION_HEADER;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_VERSION_HEADER;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_VERSION_STATUS_HEADER;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parallelized.class)
public class VersionResponseFilterTest extends SystemTestBase {

  private PomVersion current;

  @Before
  public void setUp() throws Exception {
    startDefaultMaster();
    current = PomVersion.parse(Version.POM_VERSION);

    // Wait for master to come up
    Polling.await(1, TimeUnit.MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          return doVersionRequest(current.toString());
        } catch (IOException e) {
          return null;
        }
      }
    });
  }

  @Test
  public void testEqual() throws Exception {
    final HttpURLConnection connection = doVersionRequest(Version.POM_VERSION);
    assertResponseCodeType(connection, 2);
    assertServerVersion(connection);
    assertVersionStatus(connection, "EQUAL");
  }

  private void assertResponseCodeType(final HttpURLConnection connection, final int hc)
      throws IOException {
    assertEquals(hc, connection.getResponseCode() / 100);
  }

  @Test
  public void testMajorVersion() throws Exception {
    final String newVersion = (current.getMajor() + 1) + ".0.0";
    final HttpURLConnection connection = doVersionRequest(newVersion);
    assertResponseCodeType(connection, 4);
    assertServerVersion(connection);
    assertVersionStatus(connection, "INCOMPATIBLE");
  }

  @Test
  public void testPatchVersion() throws Exception {
    final String newVersion = current.getMajor() + "." + current.getMinor() + "."
                              + (current.getPatch() + 1);
    final HttpURLConnection connection = doVersionRequest(newVersion);
    assertResponseCodeType(connection, 2);
    assertServerVersion(connection);
    assertVersionStatus(connection, "COMPATIBLE");
  }

  @Test
  public void testMinorVersion() throws Exception {
    final String newVersion = current.getMajor() + "." + (current.getMinor() + 1) + ".0";
    final HttpURLConnection connection = doVersionRequest(newVersion);
    assertResponseCodeType(connection, 2);
    assertServerVersion(connection);
    assertVersionStatus(connection, "MAYBE");
  }

  @Test
  public void testMalformed() throws Exception {
    final HttpURLConnection connection = doVersionRequest("deadbeef");
    assertResponseCodeType(connection, 4);
    assertServerVersion(connection);
    assertVersionStatus(connection, "INVALID");
  }

  @Test
  public void testMissing() throws Exception {
    final HttpURLConnection connection = doVersionRequest(null);
    assertResponseCodeType(connection, 2);
    assertServerVersion(connection);
    assertVersionStatus(connection, "MISSING");
  }

  private void assertVersionStatus(final HttpURLConnection connection, final String status) {
    assertEquals(status, connection.getHeaderField(HELIOS_VERSION_STATUS_HEADER));
  }

  private void assertServerVersion(final HttpURLConnection connection) {
    assertEquals(POM_VERSION, connection.getHeaderField(HELIOS_SERVER_VERSION_HEADER));
  }

  private HttpURLConnection doVersionRequest(String version) throws IOException {
    final Map<String, List<String>> headers = Maps.newHashMap();
    headers.put("Content-Type", asList("application/json"));
    headers.put("Charset", asList("utf-8"));
    if (version != null) {
      headers.put(HELIOS_VERSION_HEADER, asList(version));
    }
    final URI uri = URI.create(getMasterEndpoint() + "/version");
    final HttpURLConnection connection = connect(uri, headers);
    return connection;
  }

  private HttpURLConnection connect(final URI uri, final Map<String, List<String>> headers)
      throws IOException {
    final HttpURLConnection connection;
    connection = (HttpURLConnection) uri.toURL().openConnection();
    connection.setInstanceFollowRedirects(false);
    for (Map.Entry<String, List<String>> header : headers.entrySet()) {
      for (final String value : header.getValue()) {
        connection.addRequestProperty(header.getKey(), value);
      }
    }
    connection.setRequestMethod("GET");
    connection.getResponseCode();
    return connection;
  }
}

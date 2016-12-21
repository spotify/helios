/*
 * Copyright (c) 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.agent;


import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.spotify.helios.common.descriptors.SecretVolumeRequest;
import com.spotify.helios.common.descriptors.Secrets;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSecretVolumeManagerTest {

  private static OkHttpClient client = new OkHttpClient();
  private static SmallFileReader reader = mock(SmallFileReader.class);
  private static String path = "/host/secrets";

  private final MockWebServer server = new MockWebServer();
  private DefaultSecretVolumeManager manager;

  @Before
  public void setUp() throws Exception {
    server.start();
    manager = new DefaultSecretVolumeManager(client, reader, server.url("/").toString(), path);
  }

  @After
  public void tearDown() throws Exception {
    server.shutdown();
  }

  @Test
  public void testCreate() throws Exception {
    final String id = "derp";
    final Secrets.Source source = Secrets.Source.TALOS;
    final String pemData = "PEMPEMPEM";

    doReturn(pemData).when(reader).readToString(anyString());

    final String containerPath = "/container/secrets";
    final Secrets secrets = Secrets.create(source, containerPath, "/cert", "/key");

    server.enqueue(new MockResponse().setResponseCode(200));
    assertEquals(path + '/' + id, manager.create(id, secrets));

    final RecordedRequest request = server.takeRequest();
    assertEquals("/" + id, request.getPath());
    assertEquals("POST", request.getMethod());

    final Map<String, List<String>> tags = emptyMap();
    final SecretVolumeRequest svr = SecretVolumeRequest.create(id, source, pemData, pemData);
    assertEquals(svr.toJsonString(), request.getBody().readUtf8());
  }

  @Test
  public void testDestroy() throws Exception {
    final String id = "derp";
    server.enqueue(new MockResponse().setResponseCode(200));
    manager.destroy(id);

    final RecordedRequest request = server.takeRequest();
    assertEquals("/" + id, request.getPath());
    assertEquals("DELETE", request.getMethod());
  }
}

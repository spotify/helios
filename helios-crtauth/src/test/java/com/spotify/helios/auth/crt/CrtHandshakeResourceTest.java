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

package com.spotify.helios.auth.crt;

import com.spotify.crtauth.CrtAuthServer;
import com.spotify.crtauth.exceptions.ProtocolVersionException;
import com.sun.jersey.api.client.ClientResponse;

import org.junit.Rule;
import org.junit.Test;

import io.dropwizard.testing.junit.ResourceTestRule;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrtHandshakeResourceTest {

  public static final String CHAP_HEADER = "X-CHAP";
  private CrtAuthServer authServer = mock(CrtAuthServer.class);

  @Rule
  public final ResourceTestRule resources = ResourceTestRule.builder()
      .addResource(new CrtHandshakeResource(authServer))
      .build();

  @Test
  public void initialChallengeRequest_Success() throws Exception {
    final String requestBlob = "an-encoded-username";
    when(authServer.createChallenge(requestBlob)).thenReturn("the challenge");

    final ClientResponse response = resources.client().resource("/_auth")
        .header(CHAP_HEADER, "request:" + requestBlob)
        .get(ClientResponse.class);

    assertThat(response.getStatus(), is(200));
    assertThat(response.getHeaders().get(CHAP_HEADER), contains("challenge:the challenge"));
  }

  @Test
  public void initialChallengeRequest_NoUsernameRequest() {
    final ClientResponse response = resources.client().resource("/_auth")
        .get(ClientResponse.class);

    assertThat(response.getStatus(), is(400));
    assertThat(response.getEntity(String.class), is("Bad handshake"));
  }

  @Test
  public void initialChallengeRequest_ProtocolException() throws Exception {
    final String requestBlob = "an-encoded-username";
    when(authServer.createChallenge(requestBlob)).thenThrow(new ProtocolVersionException("bad!"));

    final ClientResponse response = resources.client().resource("/_auth")
        .header(CHAP_HEADER, "request:" + requestBlob)
        .get(ClientResponse.class);

    assertThat(response.getStatus(), is(400));
  }

  @Test
  public void initialChallengeRequest_FormatException() throws Exception {
    final String requestBlob = "an-encoded-username";
    when(authServer.createChallenge(requestBlob)).thenThrow(new IllegalArgumentException());

    final ClientResponse response = resources.client().resource("/_auth")
        .header(CHAP_HEADER, "request:" + requestBlob)
        .get(ClientResponse.class);

    assertThat(response.getStatus(), is(400));
  }

  @Test
  public void responseToChallenge_Success() throws Exception {
    final String requestBlob = "the signed version of the challenge";
    when(authServer.createToken(requestBlob)).thenReturn("your secret token");

    final ClientResponse response = resources.client().resource("/_auth")
        .header(CHAP_HEADER, "response:" + requestBlob)
        .get(ClientResponse.class);

    assertThat(response.getStatus(), is(200));
    assertThat(response.getHeaders().get(CHAP_HEADER), contains("token:your secret token"));
  }

  @Test
  public void responseToChallenge_ProtocolException() throws Exception {
    final String requestBlob = "the signed version of the challenge";
    when(authServer.createToken(requestBlob)).thenThrow(new ProtocolVersionException("no!"));

    final ClientResponse response = resources.client().resource("/_auth")
        .header(CHAP_HEADER, "response:" + requestBlob)
        .get(ClientResponse.class);

    assertThat(response.getStatus(), is(400));
  }

  @Test
  public void responseToChallenge_FormatException() throws Exception {
    final String requestBlob = "the signed version of the challenge";
    when(authServer.createToken(requestBlob)).thenThrow(new IllegalArgumentException("uh oh"));

    final ClientResponse response = resources.client().resource("/_auth")
        .header(CHAP_HEADER, "response:" + requestBlob)
        .get(ClientResponse.class);

    assertThat(response.getStatus(), is(400));
  }
}
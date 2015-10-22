/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.crtauth.CrtAuthClient;
import com.spotify.crtauth.exceptions.CrtAuthException;
import com.spotify.crtauth.signer.Signer;
import com.spotify.helios.client.HeliosRequest;
import com.spotify.helios.client.RequestDispatcher;
import com.spotify.helios.client.Response;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Strings.isNullOrEmpty;

public class CrtAuthProvider implements AuthProvider {

  private static final Logger log = LoggerFactory.getLogger(AuthProviderSelector.class);

  private static final String CRT_HEADER = "X-CHAP";
  private static final String AUTH_URI = "https://helios/_auth";
  private static final String CHALLENGE_PREFIX = "challenge";
  private static final String TOKEN_PREFIX = "token";

  private final RequestDispatcher dispatcher;
  private final String username;
  private final CrtAuthClient crtClient;
  private volatile String token;
  private final Object lock = new Object();

  public CrtAuthProvider(final RequestDispatcher dispatcher,
                         final CrtAuthClient crtClient,
                         final String username) {
    this.dispatcher = dispatcher;
    this.crtClient = crtClient;
    this.username = username;
  }

  @VisibleForTesting
  CrtAuthProvider(final RequestDispatcher dispatcher,
                  final String authServer,
                  final String username,
                  final Signer signer) {
    this.dispatcher = dispatcher;
    this.username = username;
    this.crtClient = new CrtAuthClient(signer, authServer);
  }

  @Override
  public String currentAuthorizationHeader() {
    // TODO (dxia) Should we use a lock here? It doesn't really matter if the token isn't
    // thread-safe. We should get valid tokens each time. The lock just prevents
    // another thread from doing extra work to get an extra token when it could just wait.
    if (token == null) {
      synchronized (lock) {
        try {
          token = renewAuthorizationHeader(null).get();
        } catch (InterruptedException | ExecutionException e) {
          log.error("Failed to complete crtauth handshake.");
          throw Throwables.propagate(e);
        }
      }
    }

    return token;
  }

  @Override
  public ListenableFuture<String> renewAuthorizationHeader(final String ignored) {
    final String authRequest = CrtAuthClient.createRequest(username);
    try {
      final HeliosRequest request = HeliosRequest.builder()
          .method("GET")
          .uri(new URI(AUTH_URI))
          .appendHeader(CRT_HEADER, "request:" + authRequest)
          .build();

      final ListenableFuture<Response> challengeFuture = dispatcher.request(request);

      final ListenableFuture<Response> tokenFuture =
          Futures.transform(challengeFuture, new AsyncFunction<Response, Response>() {
            @Override
            public ListenableFuture<Response> apply(@NotNull final Response response)
                throws Exception {
              checkStatus(response, 200);

              final String challenge = getHeader(response, CRT_HEADER, CHALLENGE_PREFIX);
              final String crtResponse = crtClient.createResponse(challenge);
              final HeliosRequest request = HeliosRequest.builder()
                  .method("GET")
                  .uri(new URI(AUTH_URI))
                  .appendHeader(CRT_HEADER, "response:" + crtResponse)
                  .build();

              return dispatcher.request(request);
            }
          });

      return Futures.transform(tokenFuture, new Function<Response, String>() {
        @Override
        public String apply(final Response response) {
          try {
            checkStatus(response, 200);
            final String token = getHeader(response, CRT_HEADER, TOKEN_PREFIX);
            return "chap:" + token;
          } catch (CrtAuthException e) {
            throw Throwables.propagate(e);
          }
        }
      });
    } catch (URISyntaxException e) {
      // This should never happen
      throw Throwables.propagate(e);
    }
  }

  private static void checkStatus(final Response response, final int expectedStatus)
      throws CrtAuthException {
    if (response.status() != expectedStatus) {
      throw new CrtAuthException(String.format(
          "Got a %d status code during the crtauth handshake.", response.status()));
    }
  }

  private static String getHeader(final Response response,
                                  final String headerName,
                                  final String headerValuePrefix) throws CrtAuthException {
    final String header = response.header(headerName);
    if (isNullOrEmpty(header)) {
      throw new CrtAuthException(String.format(
          "Didn't get an HTTP \"X-CHAP\" header %s during the crtauth handshake.", headerName));
    }

    final int i = header.indexOf(':');
    if (i == -1 || !header.substring(0, i).equals(headerValuePrefix)) {
      throw new CrtAuthException(String.format(
          "Got an invalid HTTP X-CHAP header of \"%s\" during the crtauth handshake "
          + "when expecting it to begin with \"%s\".",
          header, headerValuePrefix));
    }

    return header.substring(i + 1);
  }
}

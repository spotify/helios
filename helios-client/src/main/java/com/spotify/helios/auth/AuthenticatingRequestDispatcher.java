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

import com.google.common.net.HttpHeaders;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosRequest;
import com.spotify.helios.client.RequestDispatcher;
import com.spotify.helios.client.Response;

import javax.annotation.Nullable;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

public class AuthenticatingRequestDispatcher implements RequestDispatcher {

  private final RequestDispatcher delegate;
  private final AuthProvider.Factory authProviderFactory;
  private final String user;
  private AuthProvider authProvider;

  public AuthenticatingRequestDispatcher(final RequestDispatcher delegate,
                                         final AuthProvider.Factory authProviderFactory,
                                         final String user) {
    this.delegate = delegate;
    this.authProviderFactory = authProviderFactory;
    this.user = user;
  }

  @Override
  public ListenableFuture<Response> request(final HeliosRequest request) {
    // Include an Authorization header if it's currently available
    final HeliosRequest req;
    final String authHeader = authProvider != null ?
                              authProvider.currentAuthorizationHeader() : null;
    if (authHeader != null) {
      req = request.toBuilder().header(HttpHeaders.AUTHORIZATION, authHeader).build();
    } else {
      req = request;
    }

    return Futures.transform(delegate.request(req), new AsyncFunction<Response, Response>() {
      @Override
      public ListenableFuture<Response> apply(final Response response) throws Exception {
        if (response != null &&
            response.status() == HTTP_UNAUTHORIZED &&
            response.header(HttpHeaders.WWW_AUTHENTICATE) != null) {
          return authenticateAndRetry(req, response);
        } else {
          return immediateFuture(response);
        }
      }
    });
  }

  private ListenableFuture<Response> authenticateAndRetry(final HeliosRequest request,
                                                          final Response response) {
    if (authProvider == null) {
      try {
        authProvider = authProviderFactory.create(
            response.header(HttpHeaders.WWW_AUTHENTICATE), new AuthProviderContext(delegate, user));

        if (authProvider == null) {
          return immediateFailedFuture(new RuntimeException("Failed to instantiate AuthProvider"));
        }
      } catch (Exception e) {
        return immediateFailedFuture(new RuntimeException("Failed to instantiate AuthProvider", e));
      }
    }

    final ListenableFuture<Response> f = Futures.transform(
        authProvider.renewAuthorizationHeader(),
        new AsyncFunction<String, Response>() {
          @Override
          public ListenableFuture<Response> apply(final String authHeader)
              throws Exception {
            return delegate.request(
                request.toBuilder().header(HttpHeaders.AUTHORIZATION, authHeader).build());
          }
        });

    // If authentication fails, return the first (unauthorized) response we got, as opposed
    // to a failed future.
    // TODO (staffan): Better to return a failed future?
    return Futures.withFallback(f, new FutureFallback<Response>() {
      @Override
      public ListenableFuture<Response> create(final Throwable t) throws Exception {
        // TODO: Log auth failure
        return immediateFuture(response);
      }
    });
  }

  @Override
  public void close() {
    delegate.close();
  }

  private static class AuthProviderContext implements AuthProvider.Context {

    private final RequestDispatcher dispatcher;
    @Nullable private final String user;

    public AuthProviderContext(final RequestDispatcher dispatcher, @Nullable final String user) {
      this.dispatcher = dispatcher;
      this.user = user;
    }

    @Override
    public RequestDispatcher dispatcher() {
      return dispatcher;
    }

    @Override
    @Nullable public String user() {
      return user;
    }
  }
}

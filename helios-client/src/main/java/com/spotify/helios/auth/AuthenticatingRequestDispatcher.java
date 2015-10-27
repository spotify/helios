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

import com.google.common.base.Preconditions;
import com.google.common.net.HttpHeaders;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosRequest;
import com.spotify.helios.client.RequestDispatcher;
import com.spotify.helios.client.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

public class AuthenticatingRequestDispatcher implements RequestDispatcher {

  private static final Logger log = LoggerFactory.getLogger(AuthenticatingRequestDispatcher.class);

  private final RequestDispatcher delegate;
  private final AuthProvider.Factory authProviderFactory;
  private final String user;
  private final boolean eagerlyAuthenticate;
  private final String desiredScheme;
  private AuthProvider authProvider;

  public AuthenticatingRequestDispatcher(final RequestDispatcher delegate,
                                         final AuthProvider.Factory authProviderFactory,
                                         final String user) {
    this(delegate, authProviderFactory, user, false, null);
  }

  public AuthenticatingRequestDispatcher(final RequestDispatcher delegate,
                                         final AuthProvider.Factory authProviderFactory,
                                         final String user,
                                         boolean eagerlyAuthenticate,
                                         String desiredScheme) {

    Preconditions.checkArgument(!eagerlyAuthenticate || desiredScheme != null,
        "desiredScheme must be non-null if eagerlyAuthenticate=true");

    this.delegate = delegate;
    this.authProviderFactory = authProviderFactory;
    this.user = user;
    this.eagerlyAuthenticate = eagerlyAuthenticate;
    this.desiredScheme = desiredScheme;

    if (desiredScheme != null) {
      loadProvider(desiredScheme);
    }
  }

  private void loadProvider(final String scheme) {
    if (authProvider == null) {
      try {
        authProvider = authProviderFactory.create(scheme, new AuthProviderContext(delegate, user));

        if (authProvider == null) {
          throw new RuntimeException("Failed to instantiate AuthProvider");
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to instantiate AuthProvider", e);
      }
    }
  }

  @Override
  public ListenableFuture<Response> request(final HeliosRequest request) {
    // Include an Authorization header if it's currently available
    final String authHeader = authProvider != null ?
                              authProvider.currentAuthorizationHeader() : null;
    final HeliosRequest req;
    if (authHeader != null) {
      req = addAuthorizationHeader(request, authHeader);
    } else {
      req = request;
    }

    // If we don't have a header and eagerlyAuthenticate is set to true, then fire a future to get
    // the initial token, and then transform that into a delegate request.
    // Otherwise the "responseFuture" is the normal request via the delegate.

    final ListenableFuture<Response> responseFuture;
    if (eagerlyAuthenticate && authHeader == null) {
      // first fetch a token, then make the request via the delegate
      final ListenableFuture<String> tokenFuture = authProvider.renewAuthorizationHeader();

      responseFuture = Futures.transform(tokenFuture, new AsyncFunction<String, Response>() {
        @Override
        public ListenableFuture<Response> apply(final String token) throws Exception {
          final HeliosRequest reqWithHeader = addAuthorizationHeader(req, token);
          return delegate.request(reqWithHeader);
        }
      });
    } else {
      responseFuture = delegate.request(req);
    }

    // in either case from above, add a callback to handle 401 Unauthorized responses
    return Futures.transform(responseFuture, new AsyncFunction<Response, Response>() {
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

  private static HeliosRequest addAuthorizationHeader(final HeliosRequest request,
                                               final String authHeader) {
    return request.toBuilder().header(HttpHeaders.AUTHORIZATION, authHeader).build();
  }

  private static String parseScheme(Response response) {
    // TODO(staffan): Support multiple comma-separated challenges
    return response.header(HttpHeaders.WWW_AUTHENTICATE).split(" ", 2)[0];
  }

  private ListenableFuture<Response> authenticateAndRetry(final HeliosRequest request,
                                                          final Response response) {
    loadProvider(parseScheme(response));

    final ListenableFuture<Response> f = Futures.transform(
        authProvider.renewAuthorizationHeader(),
        new AsyncFunction<String, Response>() {
          @Override
          public ListenableFuture<Response> apply(final String authHeader)
              throws Exception {
            return delegate.request(addAuthorizationHeader(request, authHeader));
          }
        });

    // If authentication fails, return the first (unauthorized) response we got, as opposed
    // to a failed future.
    // TODO (staffan): Better to return a failed future?
    return Futures.withFallback(f, new FutureFallback<Response>() {
      @Override
      public ListenableFuture<Response> create(final Throwable t) throws Exception {
        log.error("Authentication error: {}", t);
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

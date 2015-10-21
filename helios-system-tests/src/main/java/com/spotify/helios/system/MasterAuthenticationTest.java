/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.system;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;

import com.spotify.helios.auth.AuthenticationPlugin;
import com.spotify.helios.auth.Authenticator;
import com.spotify.helios.auth.HeliosUser;
import com.sun.jersey.api.core.HttpRequestContext;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.jersey.setup.JerseyEnvironment;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests of authentication within Helios masters. */
public class MasterAuthenticationTest extends SystemTestBase {

  @Test
  public void authenticationEnabled() throws Exception {
    // use a new authentication scheme since BasicAuth requires setting up some environment
    // variables, which isn't simple to pass through startDefaultMaster
    startDefaultMaster("--auth-scheme", "fixed-password");

    final String uri = masterEndpoint() + "/masters";
    // expect an unauthenticated GET to return 401 Unauthorized
    try (CloseableHttpResponse response = httpClient.execute(new HttpGet(uri))) {
      assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_UNAUTHORIZED));

      final Header[] headers = response.getHeaders(HttpHeaders.WWW_AUTHENTICATE);
      assertThat(headers, arrayWithSize(1));
      assertThat(headers[0].getValue(), is("fixed-password"));
    }

    final HttpGet authorizedGet = new HttpGet(uri);
    authorizedGet.addHeader(HttpHeaders.AUTHORIZATION, "secret123");

    try (CloseableHttpResponse response = httpClient.execute(authorizedGet)) {
      assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_OK));
    }
  }

  @AutoService(AuthenticationPlugin.class)
  public static class FixedPasswordAuthentication implements AuthenticationPlugin<String> {

    @Override
    public String schemeName() {
      return "fixed-password";
    }

    @Override
    public ServerAuthentication<String> serverAuthentication() {
      return new ServerAuthentication<String>() {
        @Override
        public Authenticator<String> authenticator() {
          return new Authenticator<String>() {
            @Override
            public Optional<String> extractCredentials(final HttpRequestContext request) {
              return Optional.fromNullable(request.getHeaderValue(HttpHeaders.AUTHORIZATION));
            }

            @Override
            public Optional<HeliosUser> authenticate(final String credentials)
                throws AuthenticationException {
              if ("secret123".equals(credentials)) {
                return Optional.of(new HeliosUser("the-user"));
              }
              ;
              return Optional.absent();
            }
          };
        }

        @Override
        public void registerAdditionalJerseyComponents(final JerseyEnvironment env) {
        }
      };
    }

    @Override
    public ClientAuthentication<String> clientAuthentication() {
      return null;
    }
  }
}

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
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;

import com.spotify.docker.client.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.helios.auth.AuthenticationPlugin;
import com.spotify.helios.auth.Authenticator;
import com.spotify.helios.auth.HeliosUser;
import com.spotify.helios.auth.SimpleServerAuthentication;
import com.sun.jersey.api.core.HttpRequestContext;

import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;

import io.dropwizard.auth.AuthenticationException;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/** Tests of authentication within Helios masters. */
public class MasterAuthenticationTest extends SystemTestBase {

  /**
   * Simple test that ensures that a HTTP request to /masters returns 401 Unauthorized when the
   * request has no Authorization headers, and that a second request containing the expected
   * Authorization header succeeds.
   */
  @Test
  public void authenticationEnabled() throws Exception {
    // use a new authentication scheme since BasicAuth requires setting up some environment
    // variables, which isn't simple to pass through startDefaultMaster
    startDefaultMaster("--auth-scheme", "fixed-password");

    verifyNormalRequestIsUnauthorized("/masters", "fixed-password");

    final HttpGet authorizedGet = new HttpGet(masterEndpoint() + "/masters");
    authorizedGet.addHeader(HttpHeaders.AUTHORIZATION, "secret123");

    try (CloseableHttpResponse response = httpClient.execute(authorizedGet)) {
      assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_OK));
    }
  }

  private void verifyNormalRequestIsUnauthorized(String path, String expectedScheme)
      throws IOException {

    final String uri = masterEndpoint() + path;
    // expect an unauthenticated GET to return 401 Unauthorized
    try (CloseableHttpResponse response = httpClient.execute(new HttpGet(uri))) {
      assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_UNAUTHORIZED));

      final Header[] headers = response.getHeaders(HttpHeaders.WWW_AUTHENTICATE);
      assertThat(headers, arrayWithSize(1));
      assertThat(headers[0].getValue(), is(expectedScheme));
    }
  }

  @AutoService(AuthenticationPlugin.class)
  public static class FixedPasswordAuthentication implements AuthenticationPlugin<String> {

    @Override
    public String cliSchemeName() {
      return "fixed-password";
    }

    @Override
    public String schemeName() {
      return "fixed-password";
    }

    @Override
    public ServerAuthentication<String> serverAuthentication(Map<String, String> environment) {
      return new SimpleServerAuthentication<String>() {
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
              return Optional.absent();
            }
          };
        }
      };
    }
  }

  @Test
  public void basicAuthEnabled() throws Exception {
    final Map<String, String> users = ImmutableMap.of("user1", "password2");

    final File tempFile = File.createTempFile("users", "json");
    tempFile.deleteOnExit();

    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(tempFile, users);

    final Map<String, String> env = ImmutableMap.of(
        "AUTH_BASIC_USERDB", tempFile.getAbsolutePath()
    );

    startDefaultMaster(env, "--auth-scheme", "http-basic");

    // sanity check that the auth plugin is loaded and working
    verifyNormalRequestIsUnauthorized("/masters", "Basic");

    // and now with a token
    final HttpGet request = new HttpGet(masterEndpoint() + "/masters");
    final String encoded = BaseEncoding.base64().encode("user1:password2".getBytes());
    request.addHeader(HttpHeaders.AUTHORIZATION, "Basic " + encoded);

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_OK));
    }
  }

  /**
   * Make sure that when crtauth is enabled, that the /_auth endpoint it registers does not itself
   * require an Authorization header, otherwise no one would ever be able to obtain a token in the
   * first place.
   */
  @Test
  public void crtAuthEnabled_AuthEndpointDoesNotRequireAuthentication() throws Exception {
    final Map<String, String> env = ImmutableMap.of(
        "CRTAUTH_SECRET", "sekret",
        "CRTAUTH_SERVERNAME", "foo",
        "CRTAUTH_LDAP_URL", "foo",
        "CRTAUTH_LDAP_SEARCH_PATH", "foo");

    startDefaultMaster(env, "--auth-scheme", "crtauth");

    // sanity check that the auth plugin is loaded and working
    verifyNormalRequestIsUnauthorized("/masters", "crtauth");

    // the /_auth endpoint added by crtauth should not require authentication itself
    final HttpGet request = new HttpGet(masterEndpoint() + "/_auth");
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      assertThat(response.getStatusLine().getStatusCode(), is(not(HttpStatus.SC_UNAUTHORIZED)));
    }
  }
}

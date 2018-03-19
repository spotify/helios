/*-
 * -\-\-
 * Helios Client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.helios.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.common.base.Optional;
import java.io.IOException;
import java.net.URI;
import org.junit.Test;

public class GoogleCredentialsAccessTokenSupplierTest {

  @Test
  public void testGetWhenDisabled() {
    final GoogleCredentialsAccessTokenSupplier supplier =
        new GoogleCredentialsAccessTokenSupplier(false, null, null);
    assertThat(supplier.get(), equalTo(Optional.<String>absent()));
  }

  @Test
  public void testGetWithStaticToken() {
    final AccessToken token = new AccessToken("token", null);
    final GoogleCredentialsAccessTokenSupplier supplier =
        new GoogleCredentialsAccessTokenSupplier(true, token, null);
    assertThat(supplier.get(), equalTo(Optional.of("Bearer token")));
  }

  @Test
  public void testGetWithCredentials() throws IOException {
    final AccessToken accessToken = new AccessToken("foobar", null);

    // instantiating the GoogleCredentials class directly is a big hack to workaround the fact that
    // we cannot stub the final method getAccessToken()
    final GoogleCredentials credentials = new GoogleCredentials(accessToken);

    final GoogleCredentialsAccessTokenSupplier supplier = new GoogleCredentialsAccessTokenSupplier(
        true, null, null, credentials);

    assertThat(supplier.get(), equalTo(Optional.of("Bearer foobar")));
  }
}

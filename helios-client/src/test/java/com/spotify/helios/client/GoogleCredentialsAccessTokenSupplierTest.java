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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Optional;
import org.junit.Test;

public class GoogleCredentialsAccessTokenSupplierTest {

  private final AccessToken accessToken = new AccessToken("foobar", null);

  // instantiating the GoogleCredentials class directly is a big hack to workaround the fact that
  // we cannot stub the final method getAccessToken()
  private final GoogleCredentialsAccessTokenSupplier supplier =
      new GoogleCredentialsAccessTokenSupplier(new GoogleCredentials(accessToken));

  @Test
  public void testGetWithCredentials() {
    assertThat(supplier.get(), equalTo(Optional.of("Bearer foobar")));
  }
}

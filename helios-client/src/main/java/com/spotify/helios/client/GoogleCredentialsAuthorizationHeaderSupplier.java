/*
 *  -\-\-
 * helios-client
 * --
 * Copyright (C) 2017 Spotify AB
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
 *
 */

package com.spotify.helios.client;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Optional;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuthorizationHeaderSupplier instance that uses Google OAuth2 credentials to set the header
 * value.
 */
class GoogleCredentialsAuthorizationHeaderSupplier implements AuthorizationHeaderSupplier {

  private static final Logger LOG =
      LoggerFactory.getLogger(GoogleCredentialsAuthorizationHeaderSupplier.class);

  private final GoogleCredentials credentials;

  GoogleCredentialsAuthorizationHeaderSupplier(final GoogleCredentials credentials) {
    this.credentials = credentials;
  }

  @Override
  public Optional<String> get() {
    try {
      // call getRequestMetadata to kick the if-expired-then-renew check
      credentials.getRequestMetadata(null);
      return Optional.of("Bearer " + credentials.getAccessToken().getTokenValue());
    } catch (IOException | RuntimeException e) {
      LOG.error("Exception while refreshing Google Credentials", e);
      return Optional.absent();
    }
  }
}

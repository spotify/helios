/*
 *  -\-\-
 *  helios-client
 *  --
 *  Copyright (C) 2017 Spotify AB
 *  --
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *       http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  -/-/-
 *
 */

package com.spotify.helios.client;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GoogleCredentialsAccessTokenSupplier implements AuthorizationHeaderSupplier {

  private static final Logger LOG =
      LoggerFactory.getLogger(GoogleCredentialsAccessTokenSupplier.class);

  public static final List<String> DEFAULT_SCOPES = ImmutableList.of(
      "https://www.googleapis.com/auth/cloud-platform.read-only",
      "https://www.googleapis.com/auth/userinfo.email"
  );

  private final Optional<AccessToken> staticToken;
  private final List<String> tokenScopes;
  private final Object lock = new Object();
  private GoogleCredentials credentials;

  GoogleCredentialsAccessTokenSupplier() {
    this(null, DEFAULT_SCOPES);
  }

  GoogleCredentialsAccessTokenSupplier(final AccessToken staticToken) {
    this(staticToken, DEFAULT_SCOPES);
  }

  GoogleCredentialsAccessTokenSupplier(final AccessToken staticToken,
                                       final List<String> tokenScopes) {
    this(staticToken, tokenScopes, null);
  }

  @VisibleForTesting
  GoogleCredentialsAccessTokenSupplier(final AccessToken staticToken,
                                       final List<String> tokenScopes,
                                       final GoogleCredentials credentials) {
    this.staticToken = Optional.fromNullable(staticToken);
    this.tokenScopes = tokenScopes;
    this.credentials = credentials;
  }

  @Override
  public Optional<String> get() {
    if (staticToken.isPresent()) {
      return Optional.of("Bearer " + staticToken.get().getTokenValue());
    }

    try {
      synchronized (lock) {
        if (credentials == null) {
          credentials = getCredentialsWithScopes(tokenScopes);
        }
        // call getRequestMetadata to kick the if-expired-then-renew check
        credentials.getRequestMetadata(null);
      }

      return Optional.of("Bearer " + credentials.getAccessToken().getTokenValue());
    } catch (IOException | RuntimeException e) {
      LOG.debug("Exception (possibly benign) while loading Google Credentials", e);
      return Optional.absent();
    }
  }

  /**
   * Attempt to load Google Credentials with specified scopes.
   * <ol>
   * <li>First check to see if the environment variable HELIOS_GOOGLE_CREDENTIALS is set
   * and points to a readable file</li>
   * <li>Otherwise check if Google Application Default Credentials (ADC) can be loaded</li>
   * </ol>
   *
   * <p>Note that we use a special environment variable of our own in addition to any environment
   * variable that the ADC loading uses (GOOGLE_APPLICATION_CREDENTIALS) in case there is a need
   * for the user to use the latter env var for some other purpose.
   *
   * @return Return a {@link GoogleCredentials}
   */
  private static GoogleCredentials getCredentialsWithScopes(final List<String> scopes)
      throws IOException {
    GoogleCredentials credentials = null;

    // first check whether the environment variable is set
    final String googleCredentialsPath = System.getenv("HELIOS_GOOGLE_CREDENTIALS");
    if (googleCredentialsPath != null) {
      final File file = new File(googleCredentialsPath);
      if (file.exists()) {
        try (final FileInputStream s = new FileInputStream(file)) {
          credentials = GoogleCredentials.fromStream(s);
          LOG.info("Using Google Credentials from file: " + file.getAbsolutePath());
        }
      }
    }

    // fallback to application default credentials
    if (credentials == null) {
      credentials = GoogleCredentials.getApplicationDefault();
      LOG.info("Using Google Application Default Credentials");
    }

    return scopes.isEmpty() ? credentials : credentials.createScoped(scopes);
  }
}

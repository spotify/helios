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

import static java.util.Collections.singletonList;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GoogleCredentialsAccessTokenProvider {
  private static final Logger log =
      LoggerFactory.getLogger(GoogleCredentialsAccessTokenProvider.class);

  /**
   * Attempt to load an Access Token using Google Credentials.
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
   * @return Return an AccessToken or null
   */
  static AccessToken getAccessToken(final List<String> scopes) throws IOException {
    GoogleCredentials credentials = null;

    // first check whether the environment variable is set
    final String googleCredentialsPath = System.getenv("HELIOS_GOOGLE_CREDENTIALS");
    if (googleCredentialsPath != null) {
      final File file = new File(googleCredentialsPath);
      if (file.exists()) {
        final FileInputStream s = new FileInputStream(file);
        credentials = GoogleCredentials.fromStream(s);
        log.debug("Using Google Credentials from file: " + file.getAbsolutePath());
      }
    }

    // fallback to application default credentials
    if (credentials == null) {
      credentials = GoogleCredentials.getApplicationDefault();
      log.debug("Using Google Application Default Credentials");
    }

    return getAccessToken(credentials, scopes);
  }

  @VisibleForTesting
  static AccessToken getAccessToken(final GoogleCredentials credentials,
                                    final List<String> scopes) throws IOException {
    if (credentials == null) {
      return null;
    }

    final GoogleCredentials newCredentials =
        scopes.isEmpty() ? credentials : credentials.createScoped(scopes);
    newCredentials.refresh();

    return newCredentials.getAccessToken();
  }

  static AccessToken getAccessToken() throws IOException {
    // Google Service Account Credentials require an access scope before calling `refresh()`;
    // see https://cloud.google.com/compute/docs/access/service-accounts#accesscopesiam.
    return getAccessToken(singletonList("https://www.googleapis.com/auth/cloud-platform.read-only"));
  }
}

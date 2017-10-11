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

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;
import java.util.List;
import org.junit.Test;

public class GoogleCredentialsAccessTokenProviderTest {

  @Test
  public void getAccessToken() throws Exception {
    final GoogleCredentials credentials = mock(GoogleCredentials.class);
    final GoogleCredentials scopedCredentials = mock(GoogleCredentials.class);
    when(credentials.createScoped(anyCollectionOf(String.class))).thenReturn(scopedCredentials);
    final List<String> scopes = singletonList("somescope");
    GoogleCredentialsAccessTokenProvider.getAccessToken(credentials, scopes);
    verify(credentials).createScoped(scopes);
    verify(scopedCredentials).refresh();
  }

}

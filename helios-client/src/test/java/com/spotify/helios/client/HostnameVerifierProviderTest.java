/*-
 * -\-\-
 * Helios Client
 * --
 * Copyright (C) 2016 Spotify AB
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import org.junit.Test;

public class HostnameVerifierProviderTest {

  private final HostnameVerifier delegate = mock(HostnameVerifier.class);
  private final SSLSession sslSession = mock(SSLSession.class);

  @Test
  public void testHostnameVerificationDisabled() {
    final HostnameVerifierProvider provider = new HostnameVerifierProvider(false, delegate);
    final HostnameVerifier verifier = provider.verifierFor("any.host");

    assertTrue(verifier.verify("example.com", sslSession));
    verifyNoMoreInteractions(delegate, sslSession);
  }

  @Test
  public void testHostnameVerificationEnabled() {
    final String hostname = "example.com";

    final HostnameVerifierProvider provider = new HostnameVerifierProvider(true, delegate);
    final HostnameVerifier verifier = provider.verifierFor(hostname);

    // verify that the returned provider just delegates to the delgate
    when(delegate.verify(hostname, sslSession)).thenReturn(true);
    assertTrue(verifier.verify(hostname, sslSession));

    when(delegate.verify(hostname, sslSession)).thenReturn(false);
    assertFalse(verifier.verify("foo.example.com", sslSession));
  }
}

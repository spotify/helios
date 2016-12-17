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

import com.spotify.helios.client.HttpsHandlers.SshAgentHttpsHandler;
import com.spotify.helios.client.tls.CertificateAndPrivateKey;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.file.Paths;
import java.util.Random;

import static com.google.common.io.Resources.getResource;
import static com.spotify.helios.common.Hash.sha1digest;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpsHandlersTest {

  @Test
  public void testCertificateFile() throws Exception {
    final ClientCertificatePath clientCertificatePath = new ClientCertificatePath(
        Paths.get(getResource("UIDCACert.pem").getPath()),
        Paths.get(getResource("UIDCACert.key").getPath())
    );

    final HttpsHandlers.CertificateFileHttpsHandler h =
        new HttpsHandlers.CertificateFileHttpsHandler("foo", true, clientCertificatePath);

    final CertificateAndPrivateKey pair = h.createCertificateAndPrivateKey();
    assertNotNull(pair);
    assertNotNull(pair.getCertificate());
    assertNotNull(pair.getPrivateKey());
  }

  @Test
  public void testSshAgent() throws Exception {
    final byte[] random = new byte[255];
    new Random().nextBytes(random);

    final AgentProxy proxy = mock(AgentProxy.class);
    final Identity identity = mock(Identity.class);
    when(identity.getKeyBlob()).thenReturn(random);

    when(proxy.sign(any(Identity.class), any(byte[].class))).thenAnswer(new Answer<byte[]>() {
      @Override
      public byte[] answer(InvocationOnMock invocation) throws Throwable {
        final byte[] bytesToSign = (byte[]) invocation.getArguments()[1];
        return sha1digest(bytesToSign);
      }
    });

    final SshAgentHttpsHandler h = new SshAgentHttpsHandler("foo", true, proxy, identity);

    final CertificateAndPrivateKey pair = h.createCertificateAndPrivateKey();
    assertNotNull(pair);
    assertNotNull(pair.getCertificate());
    assertNotNull(pair.getPrivateKey());
  }
}

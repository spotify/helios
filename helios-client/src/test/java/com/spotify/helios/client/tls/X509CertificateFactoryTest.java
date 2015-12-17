/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.helios.client.tls;

import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.crypto.tls.Certificate;
import org.bouncycastle.util.encoders.Base64;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;

import static com.spotify.helios.common.Hash.sha1digest;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class X509CertificateFactoryTest {

  private static final String USERNAME = "rohan";

  private static final String ROHAN_PUB_KEY =
      "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAo6Uv9Ed/6g8IEdjponMNZ/s/IC/Lebo4wgUTegF7fvByb2Jk"
      + "3ldeaNLt5Ds6jg8s1eF/5AlcN4xR844foOh85vFixgyh9bu6OceKk8rzHxYB9kqRpDgEaZzEGNAbV2EYenC07nMtGK"
      + "rcNbTtKDVA7MPChzJ3qzNW+L4MTUtac8YrTWqaUaFyjL8bSkS5cF3rtnAQXWY3Js1bQnPmtRo6ZTBltu5RtvC9p2vc"
      + "iuOID7br7s1eCGf2g1mGwdj7enr3O4TLUiTR7l7KZuM+ggQyIcoGf6PU3nTS5FgHlgrowyORqBONjye09lDw1io+n6"
      + "XnXSfO3tCOAG/kTSW2zSPaPQIDAQAB";

  private final AgentProxy agentProxy = mock(AgentProxy.class);
  private final Identity identity = mock(Identity.class);

  private PublicKey publicKey;

  @Before
  public void setUp() throws Exception {
    X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(Base64.decode(ROHAN_PUB_KEY));
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    publicKey = keyFactory.generatePublic(pubKeySpec);

    when(identity.getPublicKey()).thenReturn(publicKey);
    when(agentProxy.sign(any(Identity.class), any(byte[].class))).thenAnswer(new Answer<byte[]>() {
      @Override
      public byte[] answer(InvocationOnMock invocation) throws Throwable {
        final byte[] bytesToSign = (byte[]) invocation.getArguments()[1];
        return sha1digest(bytesToSign);
      }
    });
  }

  @Test
  public void testGet() throws Exception {
    final Certificate certificates = X509CertificateFactory.get(agentProxy, identity, USERNAME);
    assertEquals(1, certificates.getLength());

    final org.bouncycastle.asn1.x509.Certificate certificate = certificates.getCertificateAt(0);

    assertEquals(USERNAME,
                 certificate.getSubject().getRDNs(BCStyle.UID)[0].getFirst().getValue().toString());
    assertArrayEquals(publicKey.getEncoded(), certificate.getSubjectPublicKeyInfo().getEncoded());
  }

}

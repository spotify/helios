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

package com.spotify.helios.client.tls;

import static com.spotify.helios.common.Hash.sha1digest;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.refEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.cert.X509Certificate;
import java.security.spec.X509EncodedKeySpec;
import org.bouncycastle.util.encoders.Base64;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class X509CertificateFactoryTest {

  @Rule
  public TemporaryFolder cacheFolder = new TemporaryFolder();

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

  private X509CertificateFactory sut;

  @Before
  public void setUp() throws Exception {
    final X509EncodedKeySpec pubKeySpec = new X509EncodedKeySpec(Base64.decode(ROHAN_PUB_KEY));
    final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    publicKey = keyFactory.generatePublic(pubKeySpec);

    when(identity.getPublicKey()).thenReturn(publicKey);
    when(identity.getKeyBlob()).thenReturn(publicKey.getEncoded());
    when(agentProxy.sign(any(Identity.class), any(byte[].class))).thenAnswer(new Answer<byte[]>() {
      @Override
      public byte[] answer(InvocationOnMock invocation) throws Throwable {
        final byte[] bytesToSign = (byte[]) invocation.getArguments()[1];
        return sha1digest(bytesToSign);
      }
    });

    sut = new X509CertificateFactory(cacheFolder.getRoot().toPath(), 500, 5000);
  }

  @Test
  public void testGet() throws Exception {
    final CertificateAndPrivateKey certificateAndPrivateKey =
        sut.get(agentProxy, identity, USERNAME);

    assertNotNull(certificateAndPrivateKey.getCertificate());
    assertNotNull(certificateAndPrivateKey.getPrivateKey());

    final X509Certificate certificate = (X509Certificate) certificateAndPrivateKey.getCertificate();

    verify(agentProxy).sign(refEq(identity), eq(certificate.getTBSCertificate()));
    assertEquals("UID=" + USERNAME, certificate.getSubjectDN().getName());
  }

  @Test
  public void testCache() throws Exception {
    final CertificateAndPrivateKey original = sut.get(agentProxy, identity, USERNAME);

    // repeated invocations should return the exact same cert & keypair
    for (int i = 0; i < 5; i++) {
      final CertificateAndPrivateKey shouldBeFromCache = sut.get(agentProxy, identity, USERNAME);

      assertArrayEquals("certificate does not match original",
                        original.getCertificate().getEncoded(),
                        shouldBeFromCache.getCertificate().getEncoded());
      assertArrayEquals("key does not match original",
                        original.getPrivateKey().getEncoded(),
                        shouldBeFromCache.getPrivateKey().getEncoded());
    }

    // sleep for long enough that the cert expires
    Thread.sleep(5000);

    final CertificateAndPrivateKey shouldBeNew = sut.get(agentProxy, identity, USERNAME);
    assertThat("cached certificate being used past expiry",
               shouldBeNew.getCertificate().getEncoded(),
               not(equalTo(original.getCertificate().getEncoded())));
    assertThat("cached key being used past expiry",
               shouldBeNew.getPrivateKey().getEncoded(),
               not(equalTo(original.getPrivateKey().getEncoded())));
  }

  @Test
  public void testCacheWithNewUsername() throws Exception {
    final CertificateAndPrivateKey original = sut.get(agentProxy, identity, USERNAME);

    // invocation with a different username should return different cert & keypair
    final CertificateAndPrivateKey shouldBeNew = sut.get(agentProxy, identity, USERNAME + "2");
    assertThat("cached certificate being used with new username",
               shouldBeNew.getCertificate().getEncoded(),
               not(equalTo(original.getCertificate().getEncoded())));
    assertThat("cached key being used with new username",
               shouldBeNew.getPrivateKey().getEncoded(),
               not(equalTo(original.getPrivateKey().getEncoded())));
  }

}

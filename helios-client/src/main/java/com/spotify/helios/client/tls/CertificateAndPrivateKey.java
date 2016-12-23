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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;

public class CertificateAndPrivateKey {

  private final Certificate certificate;
  private final PrivateKey privateKey;

  public CertificateAndPrivateKey(final Certificate certificate, final PrivateKey privateKey) {
    checkNotNull(certificate, "certificate");
    checkNotNull(privateKey, "privateKey");

    this.certificate = certificate;
    this.privateKey = privateKey;
  }

  public Certificate getCertificate() {
    return certificate;
  }

  public PrivateKey getPrivateKey() {
    return privateKey;
  }

  public static CertificateAndPrivateKey from(final Path certPath, final Path keyPath)
      throws IOException, GeneralSecurityException {
    final CertificateFactory cf = CertificateFactory.getInstance("X.509");

    final Certificate certificate;
    try (final InputStream is = Files.newInputStream(certPath)) {
      certificate = cf.generateCertificate(is);
    }

    final Object parsedPem;
    try (final BufferedReader br = Files.newBufferedReader(keyPath, Charset.defaultCharset())) {
      parsedPem = new PEMParser(br).readObject();
    }

    final PrivateKeyInfo keyInfo;
    if (parsedPem instanceof PEMKeyPair) {
      keyInfo = ((PEMKeyPair) parsedPem).getPrivateKeyInfo();
    } else if (parsedPem instanceof PrivateKeyInfo) {
      keyInfo = (PrivateKeyInfo) parsedPem;
    } else {
      throw new UnsupportedOperationException("Unable to parse x509 certificate.");
    }

    final PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyInfo.getEncoded());
    final KeyFactory kf = KeyFactory.getInstance("RSA");

    return new CertificateAndPrivateKey(certificate, kf.generatePrivate(spec));
  }
}

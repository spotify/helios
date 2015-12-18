/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.client;

import com.google.common.base.Throwables;

import com.spotify.helios.client.tls.X509CertificateFactory;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.apache.http.ssl.SSLContexts;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Provides various implementations of {@link HttpsHandler}.
 */
class HttpsHandlers {

  static class SshAgentHttpsHandler extends CertificateHttpsHandler {

    private static final Logger log = LoggerFactory.getLogger(SshAgentHttpsHandler.class);

    private final AgentProxy agentProxy;
    private final Identity identity;

    private X509CertificateFactory.CertificateAndKeyPair certificateAndKeyPair;

    SshAgentHttpsHandler(final String user,
                         final AgentProxy agentProxy,
                         final Identity identity) {
      super(user);
      this.agentProxy = checkNotNull(agentProxy, "agentProxy");
      this.identity = checkNotNull(identity, "identity");
    }

    AgentProxy getAgentProxy() {
      return agentProxy;
    }

    Identity getIdentity() {
      return identity;
    }

    @Override
    Certificate getCertificate() throws Exception {
      return certificateAndKeyPair.getCertificate();
    }

    @Override
    PrivateKey getPrivateKey() throws Exception {
      return certificateAndKeyPair.getKeyPair().getPrivate();
    }

    @Override
    public void handle(HttpsURLConnection conn) {
      certificateAndKeyPair = X509CertificateFactory.get(agentProxy, identity, getUser());
      super.handle(conn);
    }
  }

  static class CertificateFileHttpsHandler extends CertificateHttpsHandler {

    private final Path clientCertificatePath;
    private final Path clientKeyPath;

    CertificateFileHttpsHandler(final String user,
                                final Path clientCertificatePath,
                                final Path clientKeyPath) {
      super(user);
      this.clientCertificatePath = checkNotNull(clientCertificatePath, "clientCertificatePath");
      this.clientKeyPath = checkNotNull(clientKeyPath, "clientKeyPath");
    }

    Path getClientCertificatePath() {
      return clientCertificatePath;
    }

    Path getClientKeyPath() {
      return clientKeyPath;
    }

    @Override
    Certificate getCertificate() throws Exception {
      final CertificateFactory cf = CertificateFactory.getInstance("X.509");
      return cf.generateCertificate(Files.newInputStream(clientCertificatePath));
    }

    @Override
    PrivateKey getPrivateKey() throws Exception {
      final Object pemObj = new PEMParser(
          Files.newBufferedReader(clientKeyPath, Charset.defaultCharset())).readObject();

      final PrivateKeyInfo keyInfo;
      if (pemObj instanceof PEMKeyPair) {
        keyInfo = ((PEMKeyPair) pemObj).getPrivateKeyInfo();
      } else if (pemObj instanceof PrivateKeyInfo) {
        keyInfo = (PrivateKeyInfo) pemObj;
      } else {
        throw new UnsupportedOperationException("Unable to parse x509 certificate.");
      }

      final PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyInfo.getEncoded());
      final KeyFactory kf = KeyFactory.getInstance("RSA");
      return kf.generatePrivate(spec);
    }

  }

  abstract static class CertificateHttpsHandler implements HttpsHandler {

    private static final char[] KEY_STORE_PASSWORD = "FPLSlZQuM3ZCM3SjINSKuWyPK2HeS4".toCharArray();

    private final String user;

    CertificateHttpsHandler(final String user) {
      if (isNullOrEmpty(user)) {
        throw new IllegalArgumentException("user");
      }

      this.user = user;
    }

    String getUser() {
      return user;
    }

    abstract Certificate getCertificate() throws Exception;
    abstract PrivateKey getPrivateKey() throws Exception;

    public void handle(final HttpsURLConnection conn) {
      final Certificate certificate;
      final PrivateKey privateKey;

      try {
        certificate = getCertificate();
      } catch (Exception e) {
        throw new HeliosRuntimeException("error getting client certificate", e);
      }

      try {
        privateKey = getPrivateKey();
      } catch (Exception e) {
        throw new HeliosRuntimeException("error getting private key", e);
      }

      try {
        /*
          We're creating a keystore in memory and putting the certificate & key into it.
          The keystore needs a password when we put the key into it, even though it's only going to
          exist for the lifetime of the process. So we just have some random password that we use.
         */

        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("client", certificate);
        keyStore.setKeyEntry("key", privateKey, KEY_STORE_PASSWORD, new Certificate[]{certificate});

        // build an SSLContext based on our keystore, and then get an SSLSocketFactory from that
        final SSLContext sslContext = SSLContexts.custom()
            .useProtocol("TLS")
            .loadKeyMaterial(keyStore, KEY_STORE_PASSWORD)
            .build();
        conn.setSSLSocketFactory(sslContext.getSocketFactory());
      } catch (
          CertificateException |
              IOException |
              NoSuchAlgorithmException |
              KeyStoreException |
              UnrecoverableKeyException |
              KeyManagementException e) {
        // so many dumb ways to die. see https://www.youtube.com/watch?v=IJNR2EpS0jw for more.
        throw Throwables.propagate(e);
      }
    }

  }

}

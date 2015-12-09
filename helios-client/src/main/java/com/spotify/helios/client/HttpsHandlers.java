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

import com.spotify.helios.client.tls.SshAgentSSLSocketFactory;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.apache.http.ssl.SSLContexts;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMParser;

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
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * Provides various implementations of {@link HttpsHandler}.
 */
class HttpsHandlers {

  static class SshAgentHttpsHandler implements HttpsHandler {

    private final String user;
    private final AgentProxy agentProxy;
    private final Identity identity;

    SshAgentHttpsHandler(final String user,
                         final AgentProxy agentProxy,
                         final Identity identity) {
      if (isNullOrEmpty(user)) {
        throw new IllegalArgumentException();
      }

      this.user = user;
      this.agentProxy = checkNotNull(agentProxy, "agentProxy");
      this.identity = checkNotNull(identity, "identity");
    }

    String getUser() {
      return user;
    }

    AgentProxy getAgentProxy() {
      return agentProxy;
    }

    Identity getIdentity() {
      return identity;
    }

    @Override
    public void handle(final HttpsURLConnection conn) {
      conn.setSSLSocketFactory(new SshAgentSSLSocketFactory(agentProxy, identity, user));
    }
  }

  static class CertificateFileHttpsHandler implements HttpsHandler {

    private static final char[] KEY_STORE_PASSWORD = "FPLSlZQuM3ZCM3SjINSKuWyPK2HeS4".toCharArray();

    private final String user;
    private final Path clientCertificatePath;
    private final Path clientKeyPath;

    CertificateFileHttpsHandler(final String user,
                                final Path clientCertificatePath,
                                final Path clientKeyPath) {
      if (isNullOrEmpty(user)) {
        throw new IllegalArgumentException();
      }

      this.user = user;
      this.clientCertificatePath = checkNotNull(clientCertificatePath, "clientCertificatePath");
      this.clientKeyPath = checkNotNull(clientKeyPath, "clientKeyPath");
    }

    String getUser() {
      return user;
    }

    Path getClientCertificatePath() {
      return clientCertificatePath;
    }

    Path getClientKeyPath() {
      return clientKeyPath;
    }

    @Override
    public void handle(final HttpsURLConnection conn) {
      try {
        /*
          We're creating a keystore in memory and putting the certificate & key from disk into it.
          The keystore needs a password when we put the key into it, even though it's only going to
          exist for the lifetime of the process. So we just have some random password that we use.
          Most of this code is lifted from spotify/docker-client.
         */

        final CertificateFactory cf = CertificateFactory.getInstance("X.509");
        final Certificate clientCert = cf.generateCertificate(
            Files.newInputStream(clientCertificatePath));

        final PrivateKeyInfo keyInfo = (PrivateKeyInfo) new PEMParser(
            Files.newBufferedReader(clientKeyPath, Charset.defaultCharset()))
            .readObject();

        final PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyInfo.getEncoded());
        final KeyFactory kf = KeyFactory.getInstance("RSA");
        final PrivateKey clientKey = kf.generatePrivate(spec);
        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("client", clientCert);
        keyStore.setKeyEntry("key", clientKey, KEY_STORE_PASSWORD, new Certificate[]{clientCert});

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
              InvalidKeySpecException |
              KeyStoreException |
              UnrecoverableKeyException |
              KeyManagementException e) {
        // so many dumb ways to die. see https://www.youtube.com/watch?v=IJNR2EpS0jw for more.
        throw Throwables.propagate(e);
      }
    }

  }

}

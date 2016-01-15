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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import com.spotify.helios.client.tls.X509CertificateFactory;
import com.spotify.helios.client.tls.X509CertificateFactory.CertificateAndKeyPair;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.apache.http.ssl.SSLContexts;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
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

    private final AgentProxy agentProxy;
    private final Identity identity;

    SshAgentHttpsHandler(final String user,
                         final boolean failOnCertificateError,
                         final AgentProxy agentProxy,
                         final Identity identity) {
      super(user, failOnCertificateError);
      this.agentProxy = checkNotNull(agentProxy, "agentProxy");
      this.identity = checkNotNull(identity, "identity");
    }

    @VisibleForTesting
    protected AgentProxy getAgentProxy() {
      return agentProxy;
    }

    @VisibleForTesting
    protected Identity getIdentity() {
      return identity;
    }

    @Override
    protected CertificateAndPrivateKey createCertificateAndPrivateKey() {
      final CertificateAndKeyPair certificateAndKeyPair =
          X509CertificateFactory.get(agentProxy, identity, getUser());

      return CertificateAndPrivateKey.from(certificateAndKeyPair);
    }

    @Override
    protected String getCertificateSource() {
      return "ssh-agent key: " + identity.getComment();
    }
  }

  static class CertificateFileHttpsHandler extends CertificateHttpsHandler {

    private final ClientCertificatePath clientCertificatePath;

    CertificateFileHttpsHandler(final String user,
                                final boolean failOnCertificateError,
                                final ClientCertificatePath clientCertificatePath) {
      super(user, failOnCertificateError);
      this.clientCertificatePath = checkNotNull(clientCertificatePath);
    }

    @VisibleForTesting
    protected ClientCertificatePath getClientCertificatePath() {
      return clientCertificatePath;
    }

    @Override
    protected CertificateAndPrivateKey createCertificateAndPrivateKey()
        throws IOException, GeneralSecurityException {

      final CertificateFactory cf = CertificateFactory.getInstance("X.509");
      final Path certPath = clientCertificatePath.getCertificatePath();

      final Certificate certificate;
      try (final InputStream is = Files.newInputStream(certPath)) {
        certificate = cf.generateCertificate(is);
      }

      final Object parsedPem;
      try (final BufferedReader br = Files.newBufferedReader(clientCertificatePath.getKeyPath(),
                                                             Charset.defaultCharset())) {
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

    @Override
    protected String getCertificateSource() {
      return clientCertificatePath.toString();
    }
  }

  protected abstract static class CertificateHttpsHandler implements HttpsHandler {

    private static final Logger log = LoggerFactory.getLogger(CertificateHttpsHandler.class);
    private static final char[] KEY_STORE_PASSWORD = "FPLSlZQuM3ZCM3SjINSKuWyPK2HeS4".toCharArray();

    private final String user;
    private final boolean failOnCertificateError;

    protected CertificateHttpsHandler(final String user, final boolean failOnCertificateError) {
      Preconditions.checkArgument(!isNullOrEmpty(user));
      this.user = user;
      this.failOnCertificateError = failOnCertificateError;
    }

    @VisibleForTesting
    protected String getUser() {
      return user;
    }

    /**
     * Generate the Certificate and PrivateKey that will be used in {@link
     * #handle(HttpsURLConnection)}.
     *
     * <p>The method signature is defined as throwing GeneralSecurityException because there are a
     * handful of GeneralSecurityException subclasses that can be thrown in loading a x509
     * Certificate and we handle all of them identically. </p>
     */
    protected abstract CertificateAndPrivateKey createCertificateAndPrivateKey()
        throws IOException, GeneralSecurityException;

    /**
     * Return a String describing the source of the certificate for use in error messages logged by
     * {@link #handle(HttpsURLConnection)}.
     */
    protected abstract String getCertificateSource();

    public void handle(final HttpsURLConnection conn) {
      final CertificateAndPrivateKey certificateAndPrivateKey;
      try {
        certificateAndPrivateKey = createCertificateAndPrivateKey();
      } catch (IOException | GeneralSecurityException e) {
        if (failOnCertificateError) {
          throw Throwables.propagate(e);
        } else {
          log.warn(
              "Error when setting up client certificates from {}. Error was '{}'. "
              + "No certificate will be sent with request.",
              getCertificateSource(),
              e.toString());
          log.debug("full exception from setting up ClientCertificate follows", e);
          return;
        }
      }

      final Certificate certificate = certificateAndPrivateKey.certificate;
      final PrivateKey privateKey = certificateAndPrivateKey.privateKey;

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

  @VisibleForTesting
  protected static class CertificateAndPrivateKey {
    private final Certificate certificate;
    private final PrivateKey privateKey;

    public CertificateAndPrivateKey(final Certificate certificate, final PrivateKey privateKey) {
      this.certificate = certificate;
      this.privateKey = privateKey;
    }

    public Certificate getCertificate() {
      return certificate;
    }

    public PrivateKey getPrivateKey() {
      return privateKey;
    }

    static CertificateAndPrivateKey from(
        X509CertificateFactory.CertificateAndKeyPair certificateAndKeyPair) {

      return new CertificateAndPrivateKey(
          certificateAndKeyPair.getCertificate(),
          certificateAndKeyPair.getKeyPair().getPrivate());
    }
  }
}

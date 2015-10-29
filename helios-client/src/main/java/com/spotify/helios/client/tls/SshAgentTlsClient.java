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

import com.google.common.collect.Lists;

import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.tls.Certificate;
import org.bouncycastle.crypto.tls.CertificateRequest;
import org.bouncycastle.crypto.tls.DefaultTlsClient;
import org.bouncycastle.crypto.tls.HashAlgorithm;
import org.bouncycastle.crypto.tls.SignatureAlgorithm;
import org.bouncycastle.crypto.tls.SignatureAndHashAlgorithm;
import org.bouncycastle.crypto.tls.TlsAuthentication;
import org.bouncycastle.crypto.tls.TlsCredentials;
import org.bouncycastle.crypto.tls.TlsSignerCredentials;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * A {@link org.bouncycastle.crypto.tls.TlsClient} that uses an SSH agent identity for client
 * authentication. An X509 certificate is generated based on the SSH agent identity, and the
 * handshake record is sent to the local SSH agent to be signed.
 */
class SshAgentTlsClient extends DefaultTlsClient {

  private static final SignatureAndHashAlgorithm ALGO = new SignatureAndHashAlgorithm(
      HashAlgorithm.sha1, SignatureAlgorithm.rsa);

  private final AgentProxy agentProxy;
  private final Identity identity;
  private final String username;
  private final RecordingTlsClientProtocol protocol;

  private X509Certificate[] serverCertificates;

  public SshAgentTlsClient(final AgentProxy agentProxy,
                           final Identity identity,
                           final String username,
                           final RecordingTlsClientProtocol protocol) {
    super();
    this.agentProxy = agentProxy;
    this.username = username;
    this.identity = identity;
    this.protocol = protocol;
  }

  public RecordingTlsClientProtocol getProtocol() {
    return protocol;
  }

  public X509Certificate[] getServerCertificates() {
    return serverCertificates;
  }

  @Override
  public TlsAuthentication getAuthentication() throws IOException {
    return new SshAgentTlsAuthentication();
  }

  private class SshAgentTlsAuthentication implements TlsAuthentication {

    private final Certificate certificate;

    public SshAgentTlsAuthentication() {
      certificate = X509CertificateFactory.get(agentProxy, identity, username);
    }

    @Override
    public void notifyServerCertificate(final Certificate serverCertificate)
        throws IOException {
      final JcaX509CertificateConverter converter =
          new JcaX509CertificateConverter().setProvider("BC");

      final List<X509Certificate> x509Certs = Lists.newArrayList();
      try {
        for (final org.bouncycastle.asn1.x509.Certificate cert :
            serverCertificate.getCertificateList()) {
          x509Certs.add(converter.getCertificate(new X509CertificateHolder(cert)));
        }
      } catch (CertificateException e) {
        throw new IOException("couldn't convert the server certificate", e);
      }

      serverCertificates = x509Certs.toArray(new X509Certificate[0]);
    }

    @Override
    public TlsCredentials getClientCredentials(final CertificateRequest certificateRequest)
        throws IOException {
      return new TlsSignerCredentials() {

        @Override
        public byte[] generateCertificateSignature(final byte[] hash) throws IOException {
          final SshAgentContentSigner signer = new SshAgentContentSigner(agentProxy, identity);
          signer.getOutputStream().write(protocol.getRecord());
          return signer.getSignature();
        }

        @Override
        public SignatureAndHashAlgorithm getSignatureAndHashAlgorithm() {
          return ALGO;
        }

        @Override
        public Certificate getCertificate() {
          return certificate;
        }
      };
    }
  }

}

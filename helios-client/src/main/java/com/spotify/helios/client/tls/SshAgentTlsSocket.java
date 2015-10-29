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

import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;

import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.SSLSocket;
import javax.security.cert.CertificateException;
import javax.security.cert.X509Certificate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Presents our custom TLS connection as a {@link javax.net.ssl.SSLSocket} that can be easily used
 * by {@link javax.net.ssl.HttpsURLConnection}. Only operations that are actually used by
 * HttpsURLConnection are implemented.
 */
class SshAgentTlsSocket extends SSLSocket {

  private final SshAgentTlsClient client;
  private final RecordingTlsClientProtocol protocol;
  private final Socket underlying;

  public SshAgentTlsSocket(final SshAgentTlsClient client, final Socket underlying) {
    checkNotNull(client);
    checkNotNull(underlying);

    this.client = client;
    this.protocol = client.getProtocol();
    this.underlying = underlying;
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return protocol.getOutputStream();
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return protocol.getInputStream();
  }

  @Override
  public synchronized void close() throws IOException {
    protocol.close();
  }

  @Override
  public void connect(SocketAddress endpoint) throws IOException {
    underlying.connect(endpoint);
  }

  @Override
  public void connect(SocketAddress endpoint, int timeout) throws IOException {
    underlying.connect(endpoint, timeout);
  }

  @Override
  public void bind(SocketAddress bindpoint) throws IOException {
    underlying.bind(bindpoint);
  }

  @Override
  public InetAddress getInetAddress() {
    return underlying.getInetAddress();
  }

  @Override
  public InetAddress getLocalAddress() {
    return underlying.getLocalAddress();
  }

  @Override
  public int getPort() {
    return underlying.getPort();
  }

  @Override
  public int getLocalPort() {
    return underlying.getLocalPort();
  }

  @Override
  public SocketAddress getRemoteSocketAddress() {
    return underlying.getRemoteSocketAddress();
  }

  @Override
  public SocketAddress getLocalSocketAddress() {
    return underlying.getLocalSocketAddress();
  }

  @Override
  public SocketChannel getChannel() {
    return underlying.getChannel();
  }

  @Override
  public void setTcpNoDelay(boolean on) throws SocketException {
    underlying.setTcpNoDelay(on);
  }

  @Override
  public boolean getTcpNoDelay() throws SocketException {
    return underlying.getTcpNoDelay();
  }

  @Override
  public void setSoLinger(boolean on, int linger) throws SocketException {
    underlying.setSoLinger(on, linger);
  }

  @Override
  public int getSoLinger() throws SocketException {
    return underlying.getSoLinger();
  }

  @Override
  public void sendUrgentData(int data) throws IOException {
    underlying.sendUrgentData(data);
  }

  @Override
  public void setOOBInline(boolean on) throws SocketException {
    underlying.setOOBInline(on);
  }

  @Override
  public boolean getOOBInline() throws SocketException {
    return underlying.getOOBInline();
  }

  @Override
  public void setSoTimeout(int timeout) throws SocketException {
    underlying.setSoTimeout(timeout);
  }

  @Override
  public int getSoTimeout() throws SocketException {
    return underlying.getSoTimeout();
  }

  @Override
  public void setSendBufferSize(int size) throws SocketException {
    underlying.setSendBufferSize(size);
  }

  @Override
  public int getSendBufferSize() throws SocketException {
    return underlying.getSendBufferSize();
  }

  @Override
  public void setReceiveBufferSize(int size) throws SocketException {
    underlying.setReceiveBufferSize(size);
  }

  @Override
  public int getReceiveBufferSize() throws SocketException {
    return underlying.getReceiveBufferSize();
  }

  @Override
  public void setKeepAlive(boolean on) throws SocketException {
    underlying.setKeepAlive(on);
  }

  @Override
  public boolean getKeepAlive() throws SocketException {
    return underlying.getKeepAlive();
  }

  @Override
  public void setTrafficClass(int tc) throws SocketException {
    underlying.setTrafficClass(tc);
  }

  @Override
  public int getTrafficClass() throws SocketException {
    return underlying.getTrafficClass();
  }

  @Override
  public void setReuseAddress(boolean on) throws SocketException {
    underlying.setReuseAddress(on);
  }

  @Override
  public boolean getReuseAddress() throws SocketException {
    return underlying.getReuseAddress();
  }

  @Override
  public void shutdownInput() throws IOException {
    underlying.shutdownInput();
  }

  @Override
  public void shutdownOutput() throws IOException {
    underlying.shutdownOutput();
  }

  @Override
  public boolean isConnected() {
    return underlying.isConnected();
  }

  @Override
  public boolean isBound() {
    return underlying.isBound();
  }

  @Override
  public boolean isClosed() {
    return underlying.isClosed();
  }

  @Override
  public boolean isInputShutdown() {
    return underlying.isInputShutdown();
  }

  @Override
  public boolean isOutputShutdown() {
    return underlying.isOutputShutdown();
  }

  @Override
  public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
    underlying.setPerformancePreferences(connectionTime, latency, bandwidth);
  }

  @Override
  public SSLSession getSession() {
    return new SSLSession() {
      @Override
      public byte[] getId() {
        return new byte[0];
      }

      @Override
      public SSLSessionContext getSessionContext() {
        return null;
      }

      @Override
      public long getCreationTime() {
        return 0;
      }

      @Override
      public long getLastAccessedTime() {
        return 0;
      }

      @Override
      public void invalidate() {

      }

      @Override
      public boolean isValid() {
        return false;
      }

      @Override
      public void putValue(String s, Object o) {

      }

      @Override
      public Object getValue(String s) {
        return null;
      }

      @Override
      public void removeValue(String s) {

      }

      @Override
      public String[] getValueNames() {
        return new String[0];
      }

      @Override
      public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
        return client.getServerCertificates();
      }

      @Override
      public Certificate[] getLocalCertificates() {
        return new Certificate[0];
      }

      @Override
      public X509Certificate[] getPeerCertificateChain() throws SSLPeerUnverifiedException {
        final Certificate[] certificates = client.getServerCertificates();
        final X509Certificate[] x509Certificates = new X509Certificate[certificates.length];

        for (int i = 0; i < certificates.length; i++) {
          try {
            x509Certificates[i] = X509Certificate.getInstance(certificates[i].getEncoded());
          } catch (CertificateException | CertificateEncodingException e) {
            throw Throwables.propagate(e);
          }
        }

        return x509Certificates;
      }

      @Override
      public Principal getPeerPrincipal() throws SSLPeerUnverifiedException {
        final X509Certificate[] x509Certificates = getPeerCertificateChain();
        if ((x509Certificates != null) && (x509Certificates.length > 0)) {
          return x509Certificates[0].getSubjectDN();
        } else {
          return null;
        }
      }

      @Override
      public Principal getLocalPrincipal() {
        return null;
      }

      @Override
      public String getCipherSuite() {
        return "";
      }

      @Override
      public String getProtocol() {
        return client.getClientVersion().toString();
      }

      @Override
      public String getPeerHost() {
        return underlying.getInetAddress().getHostName();
      }

      @Override
      public int getPeerPort() {
        return underlying.getPort();
      }

      @Override
      public int getPacketBufferSize() {
        return 0;
      }

      @Override
      public int getApplicationBufferSize() {
        return 0;
      }
    };
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return new String[0];
  }

  @Override
  public String[] getEnabledCipherSuites() {
    return new String[0];
  }

  @Override
  public void setEnabledCipherSuites(String[] strings) {
  }

  @Override
  public String[] getSupportedProtocols() {
    return new String[0];
  }

  @Override
  public String[] getEnabledProtocols() {
    return new String[0];
  }

  @Override
  public void setEnabledProtocols(String[] strings) {
  }

  @Override
  public void addHandshakeCompletedListener(HandshakeCompletedListener handshakeCompletedListener) {
  }

  @Override
  public void removeHandshakeCompletedListener(
      HandshakeCompletedListener handshakeCompletedListener) {
  }

  @Override
  public void startHandshake() throws IOException {
  }

  @Override
  public void setUseClientMode(boolean b) {
  }

  @Override
  public boolean getUseClientMode() {
    return false;
  }

  @Override
  public void setNeedClientAuth(boolean b) {
  }

  @Override
  public boolean getNeedClientAuth() {
    return false;
  }

  @Override
  public void setWantClientAuth(boolean b) {
  }

  @Override
  public boolean getWantClientAuth() {
    return false;
  }

  @Override
  public void setEnableSessionCreation(boolean b) {
  }

  @Override
  public boolean getEnableSessionCreation() {
    return false;
  }
}

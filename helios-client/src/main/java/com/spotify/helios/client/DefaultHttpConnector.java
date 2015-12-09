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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.Json;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

// TODO (mbrown): rename
public class DefaultHttpConnector implements HttpConnector {

  private static final Logger log = LoggerFactory.getLogger(DefaultHttpConnector.class);

  private final EndpointIterator endpointIterator;
  private final HostnameVerifierProvider hostnameVerifierProvider;

  private final int httpTimeoutMillis;
  private HttpsHandler httpsHandler;

  public DefaultHttpConnector(final EndpointIterator endpointIterator,
                              final int httpTimeoutMillis,
                              final boolean sslHostnameVerificationEnabled) {
    this.endpointIterator = endpointIterator;
    this.httpTimeoutMillis = httpTimeoutMillis;
    this.hostnameVerifierProvider =
        new HostnameVerifierProvider(sslHostnameVerificationEnabled, new DefaultHostnameVerifier());
    this.httpsHandler = new NoopHttpsHandler();
  }

  @Override
  public HttpURLConnection connect(final URI uri, final String method, final byte[] entity,
                                   final Map<String, List<String>> headers) throws HeliosException {
    // TODO (mbrown): this shouldn't be here
    final Endpoint endpoint = endpointIterator.next();
    final String endpointHost = endpoint.getUri().getHost();

    try {
      return connect0(uri, method, entity, headers, endpointHost);
    } catch (ConnectException | SocketTimeoutException | UnknownHostException e) {
      // UnknownHostException happens if we can't resolve hostname into IP address.
      // UnknownHostException's getMessage method returns just the hostname which is a
      // useless message, so log the exception class name to provide more info.
      log.debug(e.toString());
      throw new HeliosException("Unable to connect to master", e);
    } catch (IOException e) {
      throw new HeliosException(e);
    }
  }

  private HttpURLConnection connect0(final URI ipUri, final String method, final byte[] entity,
                                     final Map<String, List<String>> headers,
                                     final String endpointHost)
      throws IOException {
    if (log.isTraceEnabled()) {
      log.trace("req: {} {} {} {} {} {}", method, ipUri, headers.size(),
          Joiner.on(',').withKeyValueSeparator("=").join(headers),
          entity.length, Json.asPrettyStringUnchecked(entity));
    } else {
      log.debug("req: {} {} {} {}", method, ipUri, headers.size(), entity.length);
    }

    final HttpURLConnection connection = (HttpURLConnection) ipUri.toURL().openConnection();
    handleHttps(connection, endpointHost);
    httpsHandler.handle(connection);

    connection.setRequestProperty("Accept-Encoding", "gzip");
    connection.setInstanceFollowRedirects(false);
    connection.setConnectTimeout(httpTimeoutMillis);
    connection.setReadTimeout(httpTimeoutMillis);
    for (Map.Entry<String, List<String>> header : headers.entrySet()) {
      for (final String value : header.getValue()) {
        connection.addRequestProperty(header.getKey(), value);
      }
    }
    if (entity.length > 0) {
      connection.setDoOutput(true);
      connection.getOutputStream().write(entity);
    }

    setRequestMethod(connection, method, connection instanceof HttpsURLConnection);

    return connection;
  }

  private void handleHttps(final HttpURLConnection connection, final String hostname) {

    if (!(connection instanceof HttpsURLConnection)) {
      return;
    }

    // We verify the TLS certificate against the original hostname since verifying against the
    // IP address will fail
    System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
    connection.setRequestProperty("Host", hostname);

    final HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
    httpsConnection.setHostnameVerifier(hostnameVerifierProvider.verifierFor(hostname));
  }

  private void setRequestMethod(final HttpURLConnection connection,
                                final String method,
                                final boolean isHttps) {
    // Nasty workaround for ancient HttpURLConnection only supporting few methods
    final Class<?> httpURLConnectionClass = connection.getClass();
    try {
      Field methodField;
      HttpURLConnection delegate;
      if (isHttps) {
        final Field delegateField = httpURLConnectionClass.getDeclaredField("delegate");
        delegateField.setAccessible(true);
        delegate = (HttpURLConnection) delegateField.get(connection);
        methodField = delegate.getClass().getSuperclass().getSuperclass().getSuperclass()
            .getDeclaredField("method");
      } else {
        delegate = connection;
        methodField = httpURLConnectionClass.getSuperclass().getDeclaredField("method");
      }

      methodField.setAccessible(true);
      methodField.set(delegate, method);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws Exception {
  }

  public void setHttpsHandler(final HttpsHandler httpsHandler) {
    this.httpsHandler = httpsHandler;
  }

  class NoopHttpsHandler implements HttpsHandler {

    @Override
    public void handle(final HttpURLConnection connection) {
    }
  }
}

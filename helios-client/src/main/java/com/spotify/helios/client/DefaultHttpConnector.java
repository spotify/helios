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
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Queues;

import com.spotify.helios.client.tls.SshAgentSSLSocketFactory;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.Json;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Deque;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.util.concurrent.TimeUnit.SECONDS;

// TODO (mbrown): rename
public class DefaultHttpConnector implements HttpConnector {

  private static final Logger log = LoggerFactory.getLogger(DefaultHttpConnector.class);

  private static final long HTTP_TIMEOUT_MILLIS = SECONDS.toMillis(10);
  // TODO (mbrown): remove
  private final String user;
  private final Optional<AgentProxy> agentProxy;
  private final List<Identity> identities;
  private final EndpointIterator endpointIterator;
  private final HostnameVerifierProvider hostnameVerifierProvider;

  public DefaultHttpConnector(final String user,
                              final Optional<AgentProxy> agentProxy,
                              final List<Identity> identities,
                              final EndpointIterator endpointIterator,
                              final boolean sslHostnameVerificationEnabled) {
    this.user = user;
    this.agentProxy = agentProxy;
    this.identities = identities;
    this.endpointIterator = endpointIterator;
    this.hostnameVerifierProvider =
        new HostnameVerifierProvider(sslHostnameVerificationEnabled, new DefaultHostnameVerifier());
  }

  @Override
  public HttpURLConnection connect(final URI uri, final String method, final byte[] entity,
                                   final Map<String, List<String>> headers) throws HeliosException {
// TODO (mbrown): this shouldn't be here
    final Endpoint endpoint = endpointIterator.next();
    final String endpointHost = endpoint.getUri().getHost();

    // convert the URI whose hostname portion is a domain name into a URI where the host is an IP
    // as we expect there to be several different IP addresses besides a common domain name
    final URI ipUri;
    try {
      ipUri = toIpUri(endpoint, uri);
    } catch (URISyntaxException e) {
      throw new HeliosException(e);
    }

    final Deque<Identity> ids;
    if (ipUri.getScheme().equalsIgnoreCase("https")) {
      ids = Queues.newArrayDeque(identities);
    } else {
      //noinspection ConstantConditions
      ids = Queues.newArrayDeque(null);
    }

    try {
      while (true) {
        final Identity identity = ids.poll();

        try {
          log.debug("connecting to {}", ipUri);

          final HttpURLConnection connection = connect0(
              ipUri, method, entity, headers, endpointHost, identity);

          final int responseCode = connection.getResponseCode();
          if (((responseCode == HTTP_FORBIDDEN) || (responseCode == HTTP_UNAUTHORIZED))
              && !identities.isEmpty()) {
            // there was some sort of security error. if we have any more SSH identities to try,
            // retry with the next available identity
            log.debug("retrying with next SSH identity since {} failed", identity.getComment());
            continue;
          }

          return connection;
        } catch (ConnectException | SocketTimeoutException | UnknownHostException e) {
          // UnknownHostException happens if we can't resolve hostname into IP address.
          // UnknownHostException's getMessage method returns just the hostname which is a
          // useless message, so log the exception class name to provide more info.
          log.debug(e.toString());
          throw new HeliosException("Unable to connect to master", e);
        }
      }
    } catch (IOException e) {
      throw new HeliosException(e);
    }
  }

  private URI toIpUri(Endpoint endpoint, URI uri) throws URISyntaxException {
    final URI endpointUri = endpoint.getUri();
    final String fullpath = endpointUri.getPath() + uri.getPath();

    final String uriScheme = endpointUri.getScheme();

    return new URI(uriScheme,
        endpointUri.getUserInfo(),
        endpoint.getIp().getHostAddress(),
        endpointUri.getPort(),
        fullpath,
        uri.getQuery(),
        null);
  }

  private HttpURLConnection connect0(final URI ipUri, final String method, final byte[] entity,
                                     final Map<String, List<String>> headers,
                                     final String hostname,
                                     final Identity identity)
      throws IOException {
    if (log.isTraceEnabled()) {
      log.trace("req: {} {} {} {} {} {}", method, ipUri, headers.size(),
          Joiner.on(',').withKeyValueSeparator("=").join(headers),
          entity.length, Json.asPrettyStringUnchecked(entity));
    } else {
      log.debug("req: {} {} {} {}", method, ipUri, headers.size(), entity.length);
    }

    final HttpURLConnection connection = (HttpURLConnection) ipUri.toURL().openConnection();

    // We verify the TLS certificate against the original hostname since verifying against the
    // IP address will fail
    if (connection instanceof HttpsURLConnection) {
      System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
      connection.setRequestProperty("Host", hostname);

      final HttpsURLConnection httpsConnection = (HttpsURLConnection) connection;
      httpsConnection.setHostnameVerifier(hostnameVerifierProvider.verifierFor(hostname));

      // TODO (mbrown): this expression feels redundant as we can't have an identity without an agentproxy
      if (!isNullOrEmpty(user) && agentProxy.isPresent() && identity != null) {
        httpsConnection.setSSLSocketFactory(
            new SshAgentSSLSocketFactory(agentProxy.get(), identity, user));
      }
    }

    connection.setRequestProperty("Accept-Encoding", "gzip");
    connection.setInstanceFollowRedirects(false);
    connection.setConnectTimeout((int) HTTP_TIMEOUT_MILLIS);
    connection.setReadTimeout((int) HTTP_TIMEOUT_MILLIS);
    for (Map.Entry<String, List<String>> header : headers.entrySet()) {
      for (final String value : header.getValue()) {
        connection.addRequestProperty(header.getKey(), value);
      }
    }
    if (entity.length > 0) {
      connection.setDoOutput(true);
      connection.getOutputStream().write(entity);
    }
    if (connection instanceof HttpsURLConnection) {
      setRequestMethod(connection, method, true);
    } else {
      setRequestMethod(connection, method, false);
    }

    final int responseCode = connection.getResponseCode();
    if (responseCode == HTTP_BAD_GATEWAY) {
      throw new ConnectException("502 Bad Gateway");
    }

    return connection;
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
}

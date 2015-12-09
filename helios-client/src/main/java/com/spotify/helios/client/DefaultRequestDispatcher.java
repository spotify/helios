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

package com.spotify.helios.client;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.client.tls.SshAgentSSLSocketFactory;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.Json;
import com.spotify.sshagentproxy.AgentProxies;
import com.spotify.sshagentproxy.AgentProxy;
import com.spotify.sshagentproxy.Identity;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.util.concurrent.TimeUnit.SECONDS;

class DefaultRequestDispatcher implements RequestDispatcher {

  private static final Logger log = LoggerFactory.getLogger(DefaultRequestDispatcher.class);

  private static final long HTTP_TIMEOUT_MILLIS = SECONDS.toMillis(10);

  private final EndpointIterator endpointIterator;
  private final ListeningExecutorService executorService;
  private final String user;
  private List<Identity> identities = Collections.emptyList();

  DefaultRequestDispatcher(final List<Endpoint> endpoints,
                           final String user,
                           final ListeningExecutorService executorService) {
    endpointIterator = EndpointIterator.of(endpoints);
    this.executorService = executorService;
    this.user = user;

    if (endpointIterator.hasHttps()) {
      try {
        identities = getSshIdentities();
      } catch (Exception e) {
        log.debug("Couldn't get identities from ssh-agent", e);
        log.warn(
            "Your Helios cluster is setup to work over HTTPS based on the Helios DNS records "
            + "found by this Helios CLI. This will provide client and server-side authentication. "
            + "The team that operates your Helios cluster may choose to make this authentication "
            + "optional for now in which case this is just a warning.\nWhen authentication is "
            + "required, you'll need to run ssh-agent and set the SSH_AUTH_SOCK environment "
            + "variable whenever you invoke this CLI."
        );
      }
    }
  }

  @Override
  public ListenableFuture<Response> request(final URI uri, final String method,
                                            final byte[] entityBytes,
                                            final Map<String, List<String>> headers) {
    return executorService.submit(new Callable<Response>() {
      @Override
      public Response call() throws Exception {
        final HttpURLConnection connection = connect(uri, method, entityBytes, headers);
        final int status = connection.getResponseCode();
        final InputStream rawStream;

        if (status / 100 != 2) {
          rawStream = connection.getErrorStream();
        } else {
          rawStream = connection.getInputStream();
        }

        final boolean gzip = isGzipCompressed(connection);
        final InputStream stream = gzip ? new GZIPInputStream(rawStream) : rawStream;
        final ByteArrayOutputStream payload = new ByteArrayOutputStream();
        if (stream != null) {
          int n;
          byte[] buffer = new byte[4096];
          while ((n = stream.read(buffer, 0, buffer.length)) != -1) {
            payload.write(buffer, 0, n);
          }
        }

        URI realUri = connection.getURL().toURI();
        if (log.isTraceEnabled()) {
          log.trace("rep: {} {} {} {} {} gzip:{}",
                    method, realUri, status, payload.size(), decode(payload), gzip);
        } else {
          log.debug("rep: {} {} {} {} gzip:{}",
                    method, realUri, status, payload.size(), gzip);
        }

        return new Response(
            method, uri, status, payload.toByteArray(),
            Collections.unmodifiableMap(Maps.newHashMap(connection.getHeaderFields())));
      }

      private boolean isGzipCompressed(final HttpURLConnection connection) {
        final List<String> encodings = connection.getHeaderFields().get("Content-Encoding");
        if (encodings == null) {
          return false;
        }
        for (String encoding : encodings) {
          if ("gzip".equals(encoding)) {
            return true;
          }
        }
        return false;
      }
    });
  }

  private String decode(final ByteArrayOutputStream payload) {
    final byte[] bytes = payload.toByteArray();
    try {
      return Json.asPrettyString(Json.read(bytes, new TypeReference<Map<String, Object>>() {
      }));
    } catch (IOException e) {
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  /**
   * Sets up a connection
   */
  private HttpURLConnection connect(final URI uri, final String method, final byte[] entity,
                                    final Map<String, List<String>> headers)
      throws URISyntaxException, IOException, TimeoutException, InterruptedException,
             HeliosException {

    final Endpoint endpoint = endpointIterator.next();
    final URI endpointUri = endpoint.getUri();
    final String fullpath = endpointUri.getPath() + uri.getPath();

    final String uriScheme = endpointUri.getScheme();

    final URI ipUri = new URI(
        uriScheme, endpointUri.getUserInfo(), endpoint.getIp().getHostAddress(),
        endpointUri.getPort(), fullpath, uri.getQuery(), null);

    final AgentProxy agentProxy;
    final Deque<Identity> ids;
    if (uriScheme.equalsIgnoreCase("https")) {
      agentProxy = AgentProxies.newInstance();
      ids = Queues.newArrayDeque(identities);
    } else {
      // Create null agentProxy and empty ids for connect0() later on to not perform TLS handshake
      agentProxy = null;
      ids = Queues.newArrayDeque();
    }

    try {
      while (true) {
        final Identity identity = ids.poll();

        try {
          log.debug("connecting to {}", ipUri);

          final HttpURLConnection connection = connect0(
              ipUri, method, entity, headers, endpointUri.getHost(), agentProxy, identity);

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
    } finally {
      if (agentProxy != null) {
        agentProxy.close();
      }
    }
  }

  private HttpURLConnection connect0(final URI ipUri, final String method, final byte[] entity,
                                     final Map<String, List<String>> headers,
                                     final String hostname, final AgentProxy agentProxy,
                                     final Identity identity)
      throws IOException {
    if (log.isTraceEnabled()) {
      log.trace("req: {} {} {} {} {} {}", method, ipUri, headers.size(),
                Joiner.on(',').withKeyValueSeparator("=").join(headers),
                entity.length, Json.asPrettyStringUnchecked(entity));
    } else {
      log.debug("req: {} {} {} {}", method, ipUri, headers.size(), entity.length);
    }

    final URLConnection urlConnection = ipUri.toURL().openConnection();
    final HttpURLConnection connection = (HttpURLConnection) urlConnection;

    // We verify the TLS certificate against the original hostname since verifying against the
    // IP address will fail
    if (urlConnection instanceof HttpsURLConnection) {
      System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
      connection.setRequestProperty("Host", hostname);

      final HttpsURLConnection httpsConnection = (HttpsURLConnection) urlConnection;
      httpsConnection.setHostnameVerifier(new HostnameVerifier() {
        @Override public boolean verify(String ip, SSLSession sslSession) {
          final String tHostname =
              hostname.endsWith(".") ? hostname.substring(0, hostname.length() - 1) : hostname;
          return new DefaultHostnameVerifier().verify(tHostname, sslSession);
        }
      });

      if (!isNullOrEmpty(user) && (agentProxy != null) && (identity != null)) {
        final SSLSocketFactory factory = new SshAgentSSLSocketFactory(agentProxy, identity, user);
        httpsConnection.setSSLSocketFactory(factory);
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
    if (urlConnection instanceof HttpsURLConnection) {
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

  @Override
  public void close() {
    executorService.shutdownNow();
  }

  private static List<Identity> getSshIdentities() throws IOException {
    final ImmutableList.Builder<Identity> builder = ImmutableList.builder();

    try (final AgentProxy agentProxy = AgentProxies.newInstance()) {
      for (final Identity identity : agentProxy.list()) {
        if (identity.getPublicKey().getAlgorithm().equals("RSA")) {
          // only RSA keys will work with our TLS implementation
          builder.add(identity);
        }
      }
    }

    return builder.build();
  }
}

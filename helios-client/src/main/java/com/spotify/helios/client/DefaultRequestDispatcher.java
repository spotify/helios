/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.Json;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.zip.GZIPInputStream;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

class DefaultRequestDispatcher implements RequestDispatcher {

  private static final Logger log = LoggerFactory.getLogger(DefaultRequestDispatcher.class);

  private static final long RETRY_TIMEOUT_MILLIS = SECONDS.toMillis(60);
  private static final long HTTP_TIMEOUT_MILLIS = SECONDS.toMillis(10);
  private static final List<String> VALID_PROTOCOLS = ImmutableList.of("http", "https");
  private static final String VALID_PROTOCOLS_STR =
      String.format("[%s]", Joiner.on("|").join(VALID_PROTOCOLS));

  private final Supplier<List<URI>> endpointSupplier;
  private final ListeningExecutorService executorService;

  public DefaultRequestDispatcher(final Supplier<List<URI>> endpointSupplier,
                                  final ListeningExecutorService executorService) {
    this.endpointSupplier = endpointSupplier;
    this.executorService = executorService;
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
   * Sets up a connection, retrying on connect failure.
   */
  private HttpURLConnection connect(final URI uri, final String method, final byte[] entity,
                                    final Map<String, List<String>> headers)
      throws URISyntaxException, IOException, TimeoutException, InterruptedException,
             HeliosException {
    final long deadline = currentTimeMillis() + RETRY_TIMEOUT_MILLIS;
    final int offset = ThreadLocalRandom.current().nextInt();
    while (currentTimeMillis() < deadline) {
      final List<URI> endpoints = endpointSupplier.get();
      if (endpoints.isEmpty()) {
        throw new RuntimeException("failed to resolve master");
      }
      log.debug("endpoint uris are {}", endpoints);

      // Resolve hostname into IPs so client will round-robin and retry for multiple A records.
      // Keep a mapping of IPs to hostnames for TLS verification.
      final List<URI> ipEndpoints = Lists.newArrayList();
      final Map<URI, URI> ipToHostnameUris = Maps.newHashMap();

      for (final URI hnUri : endpoints) {
        try {
          final InetAddress[] ips = InetAddress.getAllByName(hnUri.getHost());
          for (final InetAddress ip : ips) {
            final URI ipUri = new URI(
                hnUri.getScheme(), hnUri.getUserInfo(), ip.getHostAddress(), hnUri.getPort(),
                hnUri.getPath(), hnUri.getQuery(), hnUri.getFragment());
            ipEndpoints.add(ipUri);
            ipToHostnameUris.put(ipUri, hnUri);
          }
        } catch (UnknownHostException e) {
          log.warn("Unable to resolve hostname {} into IP address: {}", hnUri.getHost(), e);
        }
      }

      for (int i = 0; i < ipEndpoints.size() && currentTimeMillis() < deadline; i++) {
        final URI ipEndpoint = ipEndpoints.get(positive(offset + i) % ipEndpoints.size());
        final String fullpath = ipEndpoint.getPath() + uri.getPath();

        final String scheme = ipEndpoint.getScheme();
        final String host = ipEndpoint.getHost();
        final int port = ipEndpoint.getPort();
        if (!VALID_PROTOCOLS.contains(scheme) || host == null || port == -1) {
          throw new HeliosException(String.format(
              "Master endpoints must be of the form \"%s://heliosmaster.domain.net:<port>\"",
              VALID_PROTOCOLS_STR));
        }

        final URI realUri = new URI(scheme, host + ":" + port, fullpath, uri.getQuery(), null);
        try {
          log.debug("connecting to {}", realUri);
          return connect0(realUri, method, entity, headers,
                          ipToHostnameUris.get(ipEndpoint).getHost());
        } catch (ConnectException | SocketTimeoutException | UnknownHostException e) {
          // UnknownHostException happens if we can't resolve hostname into IP address.
          // UnknownHostException's getMessage method returns just the hostname which is a useless
          // message, so log the exception class name to provide more info.
          log.debug(e.toString());
          // Connecting failed, sleep a bit to avoid hammering and then try another endpoint
          Thread.sleep(200);
        }
      }
      log.warn("Failed to connect, retrying in 5 seconds.");
      Thread.sleep(5000);
    }
    throw new TimeoutException("Timed out connecting to master");
  }

  private HttpURLConnection connect0(final URI ipUri, final String method, final byte[] entity,
                                     final Map<String, List<String>> headers,
                                     final String hostname)
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
      ((HttpsURLConnection) connection).setHostnameVerifier(new HostnameVerifier() {
        @Override
        public boolean verify(String ip, SSLSession sslSession) {
          final String tHostname = hostname.endsWith(".") ?
                                   hostname.substring(0, hostname.length() - 1) : hostname;
          return new DefaultHostnameVerifier().verify(tHostname, sslSession);
        }
      });
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
    connection.getResponseCode();
    return connection;
  }

  private int positive(final int value) {
    return value < 0 ? value + Integer.MAX_VALUE : value;
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
}

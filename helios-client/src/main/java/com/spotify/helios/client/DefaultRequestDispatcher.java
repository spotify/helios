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

package com.spotify.helios.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.spotify.helios.common.Json;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connects to Helios masters via Sun HttpUrlConnection and handles special setup for HTTPS, as well
 * as use of ssh-agent for authentication.
 */
class DefaultRequestDispatcher implements RequestDispatcher {

  private static final Logger log = LoggerFactory.getLogger(DefaultRequestDispatcher.class);


  private final ListeningExecutorService executorService;
  private final HttpConnector httpConnector;
  private final boolean shutDownExecutorOnClose;

  DefaultRequestDispatcher(final HttpConnector httpConnector,
                           final ListeningExecutorService executorService,
                           final boolean shutDownExecutorOnClose) {
    this.executorService = executorService;
    this.httpConnector = httpConnector;
    this.shutDownExecutorOnClose = shutDownExecutorOnClose;
  }

  @Override
  public ListenableFuture<Response> request(final URI uri, final String method,
                                            final byte[] entityBytes,
                                            final Map<String, List<String>> headers) {
    return executorService.submit(new Callable<Response>() {
      @Override
      public Response call() throws Exception {
        final HttpURLConnection connection =
            httpConnector.connect(uri, method, entityBytes, headers);
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
          int numBytes;
          final byte[] buffer = new byte[4096];
          while ((numBytes = stream.read(buffer, 0, buffer.length)) != -1) {
            payload.write(buffer, 0, numBytes);
          }
        }

        final URI realUri = connection.getURL().toURI();
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
        for (final String encoding : encodings) {
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

  @Override
  public void close() throws IOException {
    if (shutDownExecutorOnClose) {
      executorService.shutdownNow();
    }
    httpConnector.close();
  }

}

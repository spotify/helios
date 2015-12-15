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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.common.Json;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.spotify.helios.client.AHCUtils.toArray;
import static com.spotify.helios.client.AHCUtils.toMap;

/** A RequestDispatcher that uses Apache HttpClient. */
// TODO (mbrown): this is meant to be wrapped in a builder and layered
class AHCRequestDispatcher implements RequestDispatcher {

  private static final Logger log = LoggerFactory.getLogger(AHCRequestDispatcher.class);

  private final ListeningExecutorService executorService;
  private final CloseableHttpClient httpClient;
  private final EndpointIterator endpointIterator;

  AHCRequestDispatcher(final ListeningExecutorService executorService,
                       final CloseableHttpClient httpClient,
                       final EndpointIterator endpointIterator) {
    this.executorService = executorService;
    this.httpClient = httpClient;
    this.endpointIterator = endpointIterator;
  }

  @Override
  public ListenableFuture<Response> request(final URI uri, final String method,
                                            final byte[] entityBytes,
                                            final Map<String, List<String>> headers) {

    // 1. select an endpoint from EndpointIterator
    // 2. set https handler depending on if cert args are present or else use ssh agent
    // 3a. (if using ssh-agent) for each Identity:
    //    - goto loop
    // 3b. goto loop
    //
    // loop:
    //    a. open connection to endpoint's IP
    //    b. if response is 502 BAD GATEWAY try again from #3
    //    c. if response is 401 or 403, try with next identity if we have it (goto #3)
    //
    // ssh-agent https handler:
    // - set SSLSocketFactory on connection to SshAgentSSLSocketFactory
    //
    // cert file https hander:
    // - create a custom SSLContext with an in-memory keystore pointing to the files
    // - set SSLSocketFactory to sslContext.getSocketFactory()
    //
    // common stuff:
    // - hostname verifier logic

    final Endpoint endpoint = endpointIterator.next();

    final HttpUriRequest request =
        createRequest(method, combine(uri, endpoint), headers, entityBytes);

    final HttpHost target = new HttpHost(endpoint.getIp(),
        endpoint.getUri().getPort(),
        endpoint.getUri().getScheme());

    return executorService.submit(new Callable<Response>() {
      @Override
      public Response call() throws Exception {
        log.debug("connecting to host={} with address={}", target, target.getAddress());
        logRequest();

        // TODO (mbrown): is the HttpHost argument necessary? I thought it would be to set the
        // inetaddress for the connection,
        // but we are setting the request.uri.host to the same inetaddress.hostaddress anyway
        try (final CloseableHttpResponse response = httpClient.execute(target, request)) {
          final int status = response.getStatusLine().getStatusCode();

          //read the response entity
          final ByteArrayOutputStream payload = new ByteArrayOutputStream();
          if (response.getEntity() != null) {
            response.getEntity().writeTo(payload);
          }

          logResponse(response, payload);

          // TODO (mbrown): why does the Response class need the method?
          final byte[] entity = payload.toByteArray();
          return new Response(method, uri, status, entity, toMap(response.getAllHeaders()));
        }
      }

      private void logRequest() {
        final URI uri = request.getURI();
        final int length = entityBytes != null ? entityBytes.length : 0;
        if (log.isTraceEnabled()) {
          log.trace("req: {} {} {} {} {} {}", method, uri, headers.size(),
              Joiner.on(',').withKeyValueSeparator("=").join(headers),
              length, Json.asPrettyStringUnchecked(entityBytes));
        } else {
          log.debug("req: {} {} {} {}", method, uri, headers.size(), length);
        }

      }

      private void logResponse(final CloseableHttpResponse response,
                               final ByteArrayOutputStream payload) {
        final URI realUri = request.getURI();
        final int status = response.getStatusLine().getStatusCode();
        // TODO (mbrown): not sure if it's worth checking this part
        boolean gzip = isGzip(response);

        if (log.isTraceEnabled()) {
          log.trace("rep: {} {} {} {} {} gzip:{}",
              method, realUri, status, payload.size(), decode(payload), gzip);
        } else {
          log.debug("rep: {} {} {} {} gzip:{}",
              method, realUri, status, payload.size(), gzip);
        }
      }

      private boolean isGzip(final CloseableHttpResponse response) {
        final Header[] encodingHeaders = response.getHeaders("Content-Encoding");
        if (encodingHeaders == null) {
          return false;
        }
        for (Header header : encodingHeaders) {
          if (header.getValue().equals("gzip")) {
            return true;
          }
        }
        return false;
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
    });
  }

  @NotNull
  private URI combine(final URI uri, final Endpoint endpoint) {
    final URI uriWithEndpoint;
    try {
      uriWithEndpoint = new URI(
          endpoint.getUri().getScheme(),
          endpoint.getUri().getUserInfo(),
          endpoint.getIp().getHostAddress(),
          endpoint.getUri().getPort(),
          endpoint.getUri().getPath() + uri.getPath(),
          uri.getQuery(),
          null
      );
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
    return uriWithEndpoint;
  }

  private HttpUriRequest createRequest(String method, URI uri, Map<String, List<String>> headers,
                                       byte[] entity) {
    final HttpUriRequest request;
    switch (method) {
      case "GET":
        checkEntityIsEmpty(method, entity);
        request = new HttpGet(uri);
        break;
      case "DELETE":
        checkEntityIsEmpty(method, entity);
        request = new HttpDelete(uri);
        break;
      default:
        final HttpEntityEnclosingRequestBase requestWithEntity;
        switch (method) {
          case "POST":
            requestWithEntity = new HttpPost(uri);
            break;
          case "PUT":
            requestWithEntity = new HttpPut(uri);
            break;
          case "PATCH":
            requestWithEntity = new HttpPatch(uri);
            break;
          default:
            throw new IllegalArgumentException("Unknown HTTP method: " + method);
        }
        requestWithEntity.setEntity(new ByteArrayEntity(entity));
        request = requestWithEntity;
        break;
    }

    request.setHeaders(toArray(headers));

    return request;
  }

  private static void checkEntityIsEmpty(final String method, final byte[] entity) {
    if (entity != null && entity.length > 0) {
      // TODO (mbrown): is the assumption that we don't need this correct?
      throw new IllegalArgumentException(
          "Cannot make " + method + " request with a request entity");
    }
  }

  @Override
  public void close() throws Exception {
    executorService.shutdownNow();
    httpClient.close();
  }
}

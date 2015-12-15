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

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.spotify.helios.client.EndpointsHelper.hostFor;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AHCRequestDispatcherTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private final ListeningExecutorService executor = MoreExecutors.newDirectExecutorService();
  private final CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
  private final Map<String, List<String>> emptyHeaders = Collections.emptyMap();

  private final URI baseUri = URI.create("https://helios:443");
  private final List<Endpoint> endpoints = Endpoints.of(
      baseUri,
      ImmutableList.of(
          InetAddresses.forString("192.168.0.1"),
          InetAddresses.forString("192.168.0.2")
      ));

  private AHCRequestDispatcher dispatcher =
      new AHCRequestDispatcher(executor, httpClient, EndpointIterator.of(endpoints));

  private static CloseableHttpResponse mockHttpResponse(int statusCode) {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);

    final BasicStatusLine statusLine = new BasicStatusLine(
        HttpVersion.HTTP_1_1, statusCode, "Test");

    when(response.getStatusLine()).thenReturn(statusLine);

    // default to no headers on response
    when(response.getAllHeaders()).thenReturn(new Header[]{});

    return response;
  }

  // necessary because HttpGet and friends don't implement equals()
  private static Matcher<HttpUriRequest> request(final String method, final URI uri) {
    final String desc = "A request with method=" + method + " and URI=" + uri;
    return new CustomTypeSafeMatcher<HttpUriRequest>(desc) {
      @Override
      protected boolean matchesSafely(final HttpUriRequest item) {
        return item.getMethod().equals(method) && item.getURI().equals(uri);
      }
    };
  }

  private static Matcher<HttpUriRequest> hasEntity(final byte[] bytes) {
    final String description = "Request with entity: " + Arrays.toString(bytes);
    return new CustomTypeSafeMatcher<HttpUriRequest>(description) {
      @Override
      protected boolean matchesSafely(final HttpUriRequest item) {
        if (!(item instanceof HttpEntityEnclosingRequest)) {
          return false;
        }
        final HttpEntity entity = ((HttpEntityEnclosingRequest) item).getEntity();
        if (entity.isStreaming()) {
          return false;
        }
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          entity.writeTo(baos);
        } catch (IOException impossible) {
          return false;
        }
        return Arrays.equals(bytes, baos.toByteArray());
      }
    };
  }

  @Test
  public void testSimpleGet() throws Exception {
    final URI uri = baseUri.resolve("/foo");

    final CloseableHttpResponse httpResponse = mockHttpResponse(200);

    // the response has some headers
    when(httpResponse.getAllHeaders()).thenReturn(new Header[]{new BasicHeader("foo", "bar")});

    when(httpClient.execute(argThat(hostFor(endpoints)), argThat(request("GET", uri))))
        .thenReturn(httpResponse);

    final Response response = dispatcher.request(uri, "GET", null, emptyHeaders).get();

    assertThat(response.status(), is(200));
    assertThat(response.header("foo"), is("bar"));
    assertThat(response.headers().size(), is(1));
    assertThat(response.payload(), isEmpty());
    assertThat(response.method(), is("GET"));
    assertThat(response.uri(), is(uri));
  }

  private static Matcher<byte[]> isEmpty() {
    return is(new byte[0]);
  }

  @Test
  public void testResponseHasPayload() throws Exception {
    final URI uri = baseUri.resolve("/foo");

    final CloseableHttpResponse httpResponse = mockHttpResponse(200);

    // and an entity
    final byte[] responseEntity = {7, 9, 82};
    when(httpResponse.getEntity()).thenReturn(new ByteArrayEntity(responseEntity));

    when(httpClient.execute(argThat(hostFor(endpoints)), argThat(request("GET", uri))))
        .thenReturn(httpResponse);

    final Response response = dispatcher.request(uri, "GET", null, emptyHeaders).get();

    assertThat(response.status(), is(200));
    assertThat(response.payload(), is(responseEntity));
  }

  @Test
  public void testRequestsWithEntity() throws Exception {
    final URI uri = baseUri.resolve("/foo");

    for (String method : new String[] {"PUT", "POST", "PATCH"}) {
      final CloseableHttpResponse httpResponse = mockHttpResponse(201);
      final byte[] inputBytes = new byte[]{1, 2, 3, 4};

      when(httpClient.execute(
          argThat(hostFor(endpoints)),
          argThat(allOf(request(method, uri), hasEntity(inputBytes))))
      ).thenReturn(httpResponse);

      final Response response = dispatcher.request(uri, method, inputBytes, emptyHeaders).get();

      assertThat(response.status(), is(201));
      assertThat(response.payload(), isEmpty());
      assertThat(response.method(), is(method));
    }
  }

  @Test
  public void testCannotUseEntityWithGet() throws Exception {
    exception.expect(IllegalArgumentException.class);

    final byte[] entity = {1, 2, 3};
    dispatcher.request(baseUri.resolve("/bar"), "GET", entity, emptyHeaders);
  }

  @Test
  public void testCannotUseEntityWithDelete() throws Exception {
    exception.expect(IllegalArgumentException.class);

    final byte[] entity = {1, 2, 3};
    dispatcher.request(baseUri.resolve("/bar"), "DELETE", entity, emptyHeaders);
  }

  @Test
  public void testBadHttpMethod() throws Exception {
    exception.expect(IllegalArgumentException.class);

    dispatcher.request(baseUri.resolve("/bat"), "BAD-HTTP-METHOD", null, emptyHeaders);
  }
}

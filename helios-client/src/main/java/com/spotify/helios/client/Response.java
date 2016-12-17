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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.util.List;
import java.util.Map;

class Response {

  private final String method;
  private final URI uri;
  private final int status;
  private final byte[] payload;
  private final Map<String, List<String>> headers;

  public Response(final String method, final URI uri, final int status, final byte[] payload,
                  final Map<String, List<String>> headers) {
    this.method = method;
    this.uri = uri;
    this.status = status;
    this.payload = payload;
    this.headers = headers;
  }

  @Override
  public String toString() {
    return "Response{"
           + "method='" + method + '\''
           + ", uri=" + uri
           + ", status=" + status
           + ", payload='" + decode(payload) + '\''
           + ", headers=" + headers
           + '}';
  }

  private static String decode(final byte[] payload) {
    if (payload == null) {
      return "";
    }
    final int length = Math.min(payload.length, 1024);
    return new String(payload, 0, length, UTF_8);
  }

  public String method() {
    return method;
  }

  public URI uri() {
    return uri;
  }

  public int status() {
    return status;
  }

  public byte[] payload() {
    return payload;
  }

  public Map<String, List<String>> headers() {
    return headers;
  }

  public String header(final String name) {
    final List<String> headerValues = headers.get(name);
    return headerValues == null || headerValues.isEmpty() ? null : headerValues.get(0);
  }
}

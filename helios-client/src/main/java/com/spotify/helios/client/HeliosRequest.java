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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class HeliosRequest {

  private final URI uri;
  private final String method;
  private final byte[] entity;
  private final Map<String, List<String>> headers;

  public HeliosRequest(final URI uri, final String method, final byte[] entity,
                       final Map<String, List<String>> headers) {
    this.uri = checkNotNull(uri);
    this.method = checkNotNull(method);
    this.entity = entity == null ? new byte[] {} : entity;
    this.headers = checkNotNull(headers);
  }

  public static Builder builder() {
    return new Builder();
  }

  public URI uri() {
    return uri;
  }

  public String method() {
    return method;
  }

  public byte[] entity() {
    return entity;
  }

  public Map<String, List<String>> headers() {
    return headers;
  }

  public String header(final String header) {
    final List<String> values = headers.get(header);
    return values == null || values.isEmpty() ? null : values.get(0);
  }

  public Builder toBuilder() {
    return builder()
        .uri(uri)
        .method(method)
        .entity(entity)
        .headers(headers);
  }

  @Override
  public String toString() {
    return "HeliosRequest{" +
           "uri=" + uri +
           ", method='" + method + '\'' +
           ", entity=" + Arrays.toString(entity) +
           ", headers=" + headers +
           '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HeliosRequest request = (HeliosRequest) o;

    if (!Arrays.equals(entity, request.entity)) {
      return false;
    }
    if (headers != null ? !headers.equals(request.headers) : request.headers != null) {
      return false;
    }
    if (method != null ? !method.equals(request.method) : request.method != null) {
      return false;
    }
    if (uri != null ? !uri.equals(request.uri) : request.uri != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = uri != null ? uri.hashCode() : 0;
    result = 31 * result + (method != null ? method.hashCode() : 0);
    result = 31 * result + (entity != null ? Arrays.hashCode(entity) : 0);
    result = 31 * result + (headers != null ? headers.hashCode() : 0);
    return result;
  }

  public static class Builder {

    private URI uri;
    private String method = "GET";
    private byte[] entity;
    private final Map<String, List<String>> headers = Maps.newHashMap();

    public HeliosRequest build() {
      return new HeliosRequest(uri, method, entity, ImmutableMap.copyOf(headers));
    }

    public Builder uri(final URI uri) {
      this.uri = uri;
      return this;
    }

    public Builder method(final String method) {
      this.method = method;
      return this;
    }

    public Builder entity(final byte[] entity) {
      this.entity = entity;
      return this;
    }

    public Builder headers(final Map<String, List<String>> headers) {
      this.headers.clear();
      this.headers.putAll(headers);
      return this;
    }

    public Builder header(final String header, final String value) {
      headers.put(header, Lists.newArrayList(value));
      return this;
    }

    public Builder appendHeader(final String header, final String value) {
      List<String> existing = headers.get(header);
      if (existing == null) {
        existing = Lists.newArrayList();
        headers.put(header, existing);
      }
      existing.add(value);
      return this;
    }
  }
}

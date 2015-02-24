/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.common.descriptors;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Strings.isNullOrEmpty;

public class HttpHealthCheck extends HealthCheck {

  private final String path;
  private final String port;

  public HttpHealthCheck(@JsonProperty("path") final String path,
                         @JsonProperty("port") final String port) {
    super(HTTP);
    this.path = path;
    this.port = port;
  }

  private HttpHealthCheck(HttpHealthCheck.Builder builder) {
    super(HTTP);
    path = builder.path;
    port = builder.port;
  }

  public String getPath() {
    return path;
  }

  public String getPort() {
    return port;
  }

  public static HttpHealthCheck of(final String port, final String path) {
    return newBuilder()
        .setPort(port)
        .setPath(path)
        .build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HttpHealthCheck that = (HttpHealthCheck) o;

    if (path != null ? !path.equals(that.path) : that.path != null) {
      return false;
    }
    if (port != null ? !port.equals(that.port) : that.port != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = path != null ? path.hashCode() : 0;
    result = 31 * result + (port != null ? port.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("path", path)
        .add("port", port)
        .toString();
  }

  static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String path;
    private String port;

    public String getPath() {
      return path;
    }

    public Builder setPath(final String path) {
      this.path = path;
      return this;
    }

    public String getPort() {
      return port;
    }

    public Builder setPort(final String port) {
      this.port = port;
      return this;
    }

    public HttpHealthCheck build() {
      if (isNullOrEmpty(path)) {
        throw new
            IllegalArgumentException("You must specify a URL path for an HTTP health check.");
      }
      if (!path.startsWith("/")) {
        throw new
            IllegalArgumentException("The path for an HTTP health check must begin with '/'.");
      }
      if (isNullOrEmpty(port)) {
        throw new IllegalArgumentException("You must specify a port for an HTTP health check.");
      }

      return new HttpHealthCheck(this);
    }
  }
}

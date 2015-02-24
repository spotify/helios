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

public class TcpHealthCheck extends HealthCheck {

  private final String port;

  public TcpHealthCheck(@JsonProperty("port") final String port) {
    super(TCP);
    this.port = port;
  }

  private TcpHealthCheck(TcpHealthCheck.Builder builder) {
    super(TCP);
    port = builder.port;
  }

  public String getPort() {
    return port;
  }

  public static TcpHealthCheck of(final String port) {
    return newBuilder().setPort(port).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TcpHealthCheck that = (TcpHealthCheck) o;

    if (port != null ? !port.equals(that.port) : that.port != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = port != null ? port.hashCode() : 0;
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("port", port)
        .toString();
  }

  static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String port;

    public String getPort() {
      return port;
    }

    public Builder setPort(final String port) {
      this.port = port;
      return this;
    }

    public TcpHealthCheck build() {
      if (isNullOrEmpty(port)) {
        throw new IllegalArgumentException("You must specify the name of a port you opened " +
                                           "to the container for a TCP health check.");
      }

      return new TcpHealthCheck(this);
    }
  }
}

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

package com.spotify.helios.common.descriptors;

import static com.google.common.base.Strings.isNullOrEmpty;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
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
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final TcpHealthCheck that = (TcpHealthCheck) obj;

    if (port != null ? !port.equals(that.port) : that.port != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return port != null ? port.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "TcpHealthCheck{"
           + "port='" + port + '\''
           + "} " + super.toString();
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
        throw new IllegalArgumentException("You must specify the name of a port you opened "
                                           + "to the container for a TCP health check.");
      }

      return new TcpHealthCheck(this);
    }
  }
}

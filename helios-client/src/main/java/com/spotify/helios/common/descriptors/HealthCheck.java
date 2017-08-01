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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Defines the health check for the Helios job. There are 3 types of health checks.
 *
 * <p>HTTP health check:
 *
 * <pre>
 * {
 *   "type" : "http",
 *   "path" : "/path/to/healthcheck",
 *   "port" : "http-admin"
 * }
 * </pre>
 *
 * <p>TCP health check:
 *
 * <pre>
 * {
 *   "type" : "tcp",
 *   "port" : "http-admin"
 * }
 * </pre>
 *
 * <p>`docker exec`-based health check:
 *
 * <pre>
 * {
 *   "type" : "exec",
 *   "command" : ["bash", "-c", "/usr/bin/curl 127.0.0.1:9200/_cluster/health | grep green"]
 * }
 * </pre>
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = ExecHealthCheck.class, name = HealthCheck.EXEC),
    @JsonSubTypes.Type(value = HttpHealthCheck.class, name = HealthCheck.HTTP),
    @JsonSubTypes.Type(value = TcpHealthCheck.class, name = HealthCheck.TCP) })
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class HealthCheck extends Descriptor {

  public static final String EXEC = "exec";
  public static final String HTTP = "http";
  public static final String TCP = "tcp";

  private final String type;

  HealthCheck(final String type) {
    checkNotNull(type, "type");
    checkArgument(!type.isEmpty(), "type is empty");
    this.type = type;
  }

  public String getType() {
    return type;
  }

  public static ExecHealthCheck.Builder newExecHealthCheck() {
    return ExecHealthCheck.newBuilder();
  }

  public static HttpHealthCheck.Builder newHttpHealthCheck() {
    return HttpHealthCheck.newBuilder();
  }

  public static TcpHealthCheck.Builder newTcpHealthCheck() {
    return TcpHealthCheck.newBuilder();
  }

  @Override
  public String toString() {
    return "HealthCheck{"
           + "type='" + type + '\''
           + '}';
  }
}

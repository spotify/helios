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

public class ExecHealthCheck extends HealthCheck {

  private final String command;

  public ExecHealthCheck(@JsonProperty("command") final String command) {
    super(EXEC);
    this.command = command;
  }

  private ExecHealthCheck(ExecHealthCheck.Builder builder) {
    super(EXEC);
    command = builder.command;
  }

  public String getCommand() {
    return command;
  }

  public static ExecHealthCheck of(final String command) {
    return newBuilder().setCommand(command).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExecHealthCheck that = (ExecHealthCheck) o;

    if (command != null ? !command.equals(that.command) : that.command != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = command != null ? command.hashCode() : 0;
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("command", command)
        .toString();
  }

  static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String command;

    public String getCommand() {
      return command;
    }

    public Builder setCommand(final String command) {
      this.command = command;
      return this;
    }

    public ExecHealthCheck build() {
      if (isNullOrEmpty(command)) {
        throw new IllegalArgumentException("You must specify a command for an exec health check.");
      }

      return new ExecHealthCheck(this);
    }
  }
}

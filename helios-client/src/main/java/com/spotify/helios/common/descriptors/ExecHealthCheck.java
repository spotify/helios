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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;

public class ExecHealthCheck extends HealthCheck {

  private final List<String> command;

  public ExecHealthCheck(@JsonProperty("command") final List<String> command) {
    super(EXEC);
    this.command = command;
  }

  private ExecHealthCheck(ExecHealthCheck.Builder builder) {
    super(EXEC);
    command = builder.command;
  }

  public List<String> getCommand() {
    return command;
  }

  public static ExecHealthCheck of(final String... command) {
    return ExecHealthCheck.of(Arrays.asList(command));
  }

  public static ExecHealthCheck of(final List<String> command) {
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

    final ExecHealthCheck that = (ExecHealthCheck) o;

    if (command != null ? !command.equals(that.command) : that.command != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return command != null ? command.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "ExecHealthCheck{" +
           "command=" + command +
           "} " + super.toString();
  }

  static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private List<String> command;

    public List<String> getCommand() {
      return command;
    }

    public Builder setCommand(final List<String> command) {
      this.command = command;
      return this;
    }

    public ExecHealthCheck build() {
      if (command == null || command.isEmpty()) {
        throw new IllegalArgumentException("You must specify a command for an exec health check.");
      }

      return new ExecHealthCheck(this);
    }
  }
}

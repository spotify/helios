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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The state of a deployment group.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DeploymentGroupStatus extends Descriptor {

  public enum State {
    ROLLING_OUT,
    FAILED,
    DONE,
  }

  private final State state;
  private final String error;

  private DeploymentGroupStatus(
      @JsonProperty("state") final State state,
      @JsonProperty("error") final String error) {
    this.state = checkNotNull(state, "state");
    this.error = error;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setState(state)
        .setError(error);
  }

  private DeploymentGroupStatus(final Builder builder) {
    this.state = checkNotNull(builder.state, "state");
    this.error = builder.error;
  }

  public State getState() {
    return state;
  }

  public String getError() {
    return error;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DeploymentGroupStatus that = (DeploymentGroupStatus) o;

    if (error != null ? !error.equals(that.error) : that.error != null) {
      return false;
    }
    if (state != that.state) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = state != null ? state.hashCode() : 0;
    result = 31 * result + (error != null ? error.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DeploymentGroupStatus{" +
           "state=" + state +
           ", error='" + error + '\'' +
           "}";
  }

  public static class Builder {
    private DeploymentGroupStatus.State state;
    private String error;

    public Builder setState(DeploymentGroupStatus.State state) {
      this.state = state;
      return this;
    }

    public Builder setError(String error) {
      this.error = error;
      return this;
    }

    public DeploymentGroupStatus build() {
      return new DeploymentGroupStatus(this);
    }
  }
}

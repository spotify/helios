package com.spotify.helios.agent.docker.messages;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)
public class ContainerExit {

  @JsonProperty("StatusCode") private int statusCode;

  public ContainerExit() {
  }

  public ContainerExit(final int statusCode) {
    this.statusCode = statusCode;
  }

  public int statusCode() {
    return statusCode;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ContainerExit that = (ContainerExit) o;

    if (statusCode != that.statusCode) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return statusCode;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("statusCode", statusCode)
        .toString();
  }
}

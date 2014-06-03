package com.spotify.helios.agent.docker.messages;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)
public class ContainerCreation {

  @JsonProperty("Id") private String id;
  @JsonProperty("Warnings") private List<String> warnings;

  public ContainerCreation() {
  }

  public ContainerCreation(final String id) {
    this.id = id;
  }

  public String id() {
    return id;
  }

  public List<String> getWarnings() {
    return warnings;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ContainerCreation that = (ContainerCreation) o;

    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }
    if (warnings != null ? !warnings.equals(that.warnings) : that.warnings != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (warnings != null ? warnings.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("id", id)
        .add("warnings", warnings)
        .toString();
  }
}

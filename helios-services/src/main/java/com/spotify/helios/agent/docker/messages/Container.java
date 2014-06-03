package com.spotify.helios.agent.docker.messages;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)
public class Container {

  @JsonProperty("Id") private String id;
  @JsonProperty("Names") private List<String> names;
  @JsonProperty("Image") private String image;
  @JsonProperty("Command") private String command;
  @JsonProperty("Created") private long created;
  @JsonProperty("Status") private String status;
  @JsonProperty("Ports") private List<PortMapping> ports;
  @JsonProperty("SizeRw") private long sizeRw;
  @JsonProperty("SizeRootFs") private long sizeRootFs;

  public String id() {
    return id;
  }

  public List<String> names() {
    return names;
  }

  public String image() {
    return image;
  }

  public String command() {
    return command;
  }

  public long created() {
    return created;
  }

  public String status() {
    return status;
  }

  public List<PortMapping> ports() {
    return ports;
  }

  public long sizeRw() {
    return sizeRw;
  }

  public long sizeRootFs() {
    return sizeRootFs;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final Container container = (Container) o;

    if (created != container.created) {
      return false;
    }
    if (sizeRootFs != container.sizeRootFs) {
      return false;
    }
    if (sizeRw != container.sizeRw) {
      return false;
    }
    if (command != null ? !command.equals(container.command) : container.command != null) {
      return false;
    }
    if (id != null ? !id.equals(container.id) : container.id != null) {
      return false;
    }
    if (image != null ? !image.equals(container.image) : container.image != null) {
      return false;
    }
    if (ports != null ? !ports.equals(container.ports) : container.ports != null) {
      return false;
    }
    if (status != null ? !status.equals(container.status) : container.status != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (image != null ? image.hashCode() : 0);
    result = 31 * result + (command != null ? command.hashCode() : 0);
    result = 31 * result + (int) (created ^ (created >>> 32));
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (ports != null ? ports.hashCode() : 0);
    result = 31 * result + (int) (sizeRw ^ (sizeRw >>> 32));
    result = 31 * result + (int) (sizeRootFs ^ (sizeRootFs >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("id", id)
        .add("image", image)
        .add("command", command)
        .add("created", created)
        .add("status", status)
        .add("ports", ports)
        .add("sizeRw", sizeRw)
        .add("sizeRootFs", sizeRootFs)
        .toString();
  }

  public static class PortMapping {

    @JsonProperty("PrivatePort") private int privatePort;
    @JsonProperty("PublicPort") private int publicPort;
    @JsonProperty("Type") private String type;
    @JsonProperty("IP") private String ip;

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final PortMapping that = (PortMapping) o;

      if (privatePort != that.privatePort) {
        return false;
      }
      if (publicPort != that.publicPort) {
        return false;
      }
      if (ip != null ? !ip.equals(that.ip) : that.ip != null) {
        return false;
      }
      if (type != null ? !type.equals(that.type) : that.type != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = privatePort;
      result = 31 * result + publicPort;
      result = 31 * result + (type != null ? type.hashCode() : 0);
      result = 31 * result + (ip != null ? ip.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("privatePort", privatePort)
          .add("publicPort", publicPort)
          .add("type", type)
          .add("ip", ip)
          .toString();
    }
  }
}

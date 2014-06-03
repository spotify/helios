package com.spotify.helios.agent.docker.messages;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)
public class HostConfig {

  @JsonProperty("Binds") private List<String> binds;
  @JsonProperty("ContainerIDFile") private String containerIDFile;
  @JsonProperty("LxcConf") private List<LxcConfParameter> lxcConf;
  @JsonProperty("Privileged") private boolean privileged;
  @JsonProperty("PortBindings") private Map<String, List<PortBinding>> portBindings;
  @JsonProperty("Links") private List<String> links;
  @JsonProperty("PublishAllPorts") private boolean publishAllPorts;
  @JsonProperty("Dns") private List<String> dns;
  @JsonProperty("DnsSearch") private List<String> dnsSearch;
  @JsonProperty("VolumesFrom") private List<String> volumesFrom;
  @JsonProperty("NetworkMode") private String networkMode;

  public List<String> binds() {
    return binds;
  }

  public void binds(final List<String> binds) {
    this.binds = binds;
  }

  public String containerIDFile() {
    return containerIDFile;
  }

  public void containerIDFile(final String containerIDFile) {
    this.containerIDFile = containerIDFile;
  }

  public List<LxcConfParameter> lxcConf() {
    return lxcConf;
  }

  public void lxcConf(final List<LxcConfParameter> lxcConf) {
    this.lxcConf = lxcConf;
  }

  public boolean isPrivileged() {
    return privileged;
  }

  public void privileged(final boolean privileged) {
    this.privileged = privileged;
  }

  public Map<String, List<PortBinding>> portBindings() {
    return portBindings;
  }

  public void portBindings(final Map<String, List<PortBinding>> portBindings) {
    this.portBindings = portBindings;
  }

  public List<String> links() {
    return links;
  }

  public void links(final List<String> links) {
    this.links = links;
  }

  public boolean isPublishAllPorts() {
    return publishAllPorts;
  }

  public void publishAllPorts(final boolean publishAllPorts) {
    this.publishAllPorts = publishAllPorts;
  }

  public List<String> dns() {
    return dns;
  }

  public void dns(final List<String> dns) {
    this.dns = dns;
  }

  public List<String> dnsSearch() {
    return dnsSearch;
  }

  public void dnsSearch(final List<String> dnsSearch) {
    this.dnsSearch = dnsSearch;
  }

  public List<String> volumesFrom() {
    return volumesFrom;
  }

  public void volumesFrom(final List<String> volumesFrom) {
    this.volumesFrom = volumesFrom;
  }

  public String networkMode() {
    return networkMode;
  }

  public void networkMode(final String networkMode) {
    this.networkMode = networkMode;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final HostConfig that = (HostConfig) o;

    if (privileged != that.privileged) {
      return false;
    }
    if (publishAllPorts != that.publishAllPorts) {
      return false;
    }
    if (binds != null ? !binds.equals(that.binds) : that.binds != null) {
      return false;
    }
    if (containerIDFile != null ? !containerIDFile.equals(that.containerIDFile)
                                : that.containerIDFile != null) {
      return false;
    }
    if (dns != null ? !dns.equals(that.dns) : that.dns != null) {
      return false;
    }
    if (dnsSearch != null ? !dnsSearch.equals(that.dnsSearch) : that.dnsSearch != null) {
      return false;
    }
    if (links != null ? !links.equals(that.links) : that.links != null) {
      return false;
    }
    if (lxcConf != null ? !lxcConf.equals(that.lxcConf) : that.lxcConf != null) {
      return false;
    }
    if (networkMode != null ? !networkMode.equals(that.networkMode) : that.networkMode != null) {
      return false;
    }
    if (portBindings != null ? !portBindings.equals(that.portBindings)
                             : that.portBindings != null) {
      return false;
    }
    if (volumesFrom != null ? !volumesFrom.equals(that.volumesFrom) : that.volumesFrom != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = binds != null ? binds.hashCode() : 0;
    result = 31 * result + (containerIDFile != null ? containerIDFile.hashCode() : 0);
    result = 31 * result + (lxcConf != null ? lxcConf.hashCode() : 0);
    result = 31 * result + (privileged ? 1 : 0);
    result = 31 * result + (portBindings != null ? portBindings.hashCode() : 0);
    result = 31 * result + (links != null ? links.hashCode() : 0);
    result = 31 * result + (publishAllPorts ? 1 : 0);
    result = 31 * result + (dns != null ? dns.hashCode() : 0);
    result = 31 * result + (dnsSearch != null ? dnsSearch.hashCode() : 0);
    result = 31 * result + (volumesFrom != null ? volumesFrom.hashCode() : 0);
    result = 31 * result + (networkMode != null ? networkMode.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("binds", binds)
        .add("containerIDFile", containerIDFile)
        .add("lxcConf", lxcConf)
        .add("privileged", privileged)
        .add("portBindings", portBindings)
        .add("links", links)
        .add("publishAllPorts", publishAllPorts)
        .add("dns", dns)
        .add("dnsSearch", dnsSearch)
        .add("volumesFrom", volumesFrom)
        .add("networkMode", networkMode)
        .toString();
  }

  private class LxcConfParameter {

    @JsonProperty("Key") private String key;
    @JsonProperty("Value") private String value;

    public String getKey() {
      return key;
    }

    public void setKey(final String key) {
      this.key = key;
    }

    public String getValue() {
      return value;
    }

    public void setValue(final String value) {
      this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final LxcConfParameter that = (LxcConfParameter) o;

      if (key != null ? !key.equals(that.key) : that.key != null) {
        return false;
      }
      if (value != null ? !value.equals(that.value) : that.value != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("key", key)
          .add("value", value)
          .toString();
    }
  }
}



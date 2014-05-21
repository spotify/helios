package com.spotify.helios.agent.docker.messages;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

@JsonAutoDetect(fieldVisibility = ANY, setterVisibility = NONE, getterVisibility = NONE)
public class ContainerInfo {

  @JsonProperty("ID") private String id;
  @JsonProperty("Created") private Date created;
  @JsonProperty("Path") private String path;
  @JsonProperty("Args") private List<String> args;
  @JsonProperty("Config") private ContainerConfig config;
  @JsonProperty("State") private ContainerState state;
  @JsonProperty("Image") private String image;
  @JsonProperty("NetworkSettings") private NetworkSettings networkSettings;
  @JsonProperty("ResolvConfPath") private String resolvConfPath;
  @JsonProperty("HostnamePath") private String hostnamePath;
  @JsonProperty("HostsPath") private String hostsPath;
  @JsonProperty("Name") private String name;
  @JsonProperty("Driver") private String driver;
  @JsonProperty("ExecDriver") private String execDriver;
  @JsonProperty("ProcessLabel") private String processLabel;
  @JsonProperty("MountLabel") private String mountLabel;
  @JsonProperty("Volumes") private Map<String, String> volumes;
  @JsonProperty("VolumesRW") private Map<String, Boolean> volumesRW;

  public String id() {
    return id;
  }

  public void id(final String id) {
    this.id = id;
  }

  public Date created() {
    return created;
  }

  public void created(final Date created) {
    this.created = created;
  }

  public String path() {
    return path;
  }

  public void path(final String path) {
    this.path = path;
  }

  public List<String> args() {
    return args;
  }

  public void args(final List<String> args) {
    this.args = args;
  }

  public ContainerConfig config() {
    return config;
  }

  public void config(final ContainerConfig config) {
    this.config = config;
  }

  public ContainerState state() {
    return state;
  }

  public void state(final ContainerState state) {
    this.state = state;
  }

  public String image() {
    return image;
  }

  public void image(final String image) {
    this.image = image;
  }

  public NetworkSettings networkSettings() {
    return networkSettings;
  }

  public void networkSettings(final NetworkSettings networkSettings) {
    this.networkSettings = networkSettings;
  }

  public String resolvConfPath() {
    return resolvConfPath;
  }

  public void resolvConfPath(final String resolvConfPath) {
    this.resolvConfPath = resolvConfPath;
  }

  public String hostnamePath() {
    return hostnamePath;
  }

  public void hostnamePath(final String hostnamePath) {
    this.hostnamePath = hostnamePath;
  }

  public String hostsPath() {
    return hostsPath;
  }

  public void hostsPath(final String hostsPath) {
    this.hostsPath = hostsPath;
  }

  public String name() {
    return name;
  }

  public void name(final String name) {
    this.name = name;
  }

  public String driver() {
    return driver;
  }

  public void driver(final String driver) {
    this.driver = driver;
  }

  public String execDriver() {
    return execDriver;
  }

  public void execDriver(final String execDriver) {
    this.execDriver = execDriver;
  }

  public String processLabel() {
    return processLabel;
  }

  public void processLabel(final String processLabel) {
    this.processLabel = processLabel;
  }

  public String mountLabel() {
    return mountLabel;
  }

  public void mountLabel(final String mountLabel) {
    this.mountLabel = mountLabel;
  }

  public Map<String, String> volumes() {
    return volumes;
  }

  public void volumes(final Map<String, String> volumes) {
    this.volumes = volumes;
  }

  public Map<String, Boolean> volumesRW() {
    return volumesRW;
  }

  public void volumesRW(final Map<String, Boolean> volumesRW) {
    this.volumesRW = volumesRW;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ContainerInfo that = (ContainerInfo) o;

    if (args != null ? !args.equals(that.args) : that.args != null) {
      return false;
    }
    if (config != null ? !config.equals(that.config) : that.config != null) {
      return false;
    }
    if (created != null ? !created.equals(that.created) : that.created != null) {
      return false;
    }
    if (driver != null ? !driver.equals(that.driver) : that.driver != null) {
      return false;
    }
    if (execDriver != null ? !execDriver.equals(that.execDriver) : that.execDriver != null) {
      return false;
    }
    if (hostnamePath != null ? !hostnamePath.equals(that.hostnamePath)
                             : that.hostnamePath != null) {
      return false;
    }
    if (hostsPath != null ? !hostsPath.equals(that.hostsPath) : that.hostsPath != null) {
      return false;
    }
    if (id != null ? !id.equals(that.id) : that.id != null) {
      return false;
    }
    if (image != null ? !image.equals(that.image) : that.image != null) {
      return false;
    }
    if (mountLabel != null ? !mountLabel.equals(that.mountLabel) : that.mountLabel != null) {
      return false;
    }
    if (name != null ? !name.equals(that.name) : that.name != null) {
      return false;
    }
    if (networkSettings != null ? !networkSettings.equals(that.networkSettings)
                                : that.networkSettings != null) {
      return false;
    }
    if (path != null ? !path.equals(that.path) : that.path != null) {
      return false;
    }
    if (processLabel != null ? !processLabel.equals(that.processLabel)
                             : that.processLabel != null) {
      return false;
    }
    if (resolvConfPath != null ? !resolvConfPath.equals(that.resolvConfPath)
                               : that.resolvConfPath != null) {
      return false;
    }
    if (state != null ? !state.equals(that.state) : that.state != null) {
      return false;
    }
    if (volumes != null ? !volumes.equals(that.volumes) : that.volumes != null) {
      return false;
    }
    if (volumesRW != null ? !volumesRW.equals(that.volumesRW) : that.volumesRW != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (created != null ? created.hashCode() : 0);
    result = 31 * result + (path != null ? path.hashCode() : 0);
    result = 31 * result + (args != null ? args.hashCode() : 0);
    result = 31 * result + (config != null ? config.hashCode() : 0);
    result = 31 * result + (state != null ? state.hashCode() : 0);
    result = 31 * result + (image != null ? image.hashCode() : 0);
    result = 31 * result + (networkSettings != null ? networkSettings.hashCode() : 0);
    result = 31 * result + (resolvConfPath != null ? resolvConfPath.hashCode() : 0);
    result = 31 * result + (hostnamePath != null ? hostnamePath.hashCode() : 0);
    result = 31 * result + (hostsPath != null ? hostsPath.hashCode() : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    result = 31 * result + (driver != null ? driver.hashCode() : 0);
    result = 31 * result + (execDriver != null ? execDriver.hashCode() : 0);
    result = 31 * result + (processLabel != null ? processLabel.hashCode() : 0);
    result = 31 * result + (mountLabel != null ? mountLabel.hashCode() : 0);
    result = 31 * result + (volumes != null ? volumes.hashCode() : 0);
    result = 31 * result + (volumesRW != null ? volumesRW.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("id", id)
        .add("created", created)
        .add("path", path)
        .add("args", args)
        .add("config", config)
        .add("state", state)
        .add("image", image)
        .add("networkSettings", networkSettings)
        .add("resolvConfPath", resolvConfPath)
        .add("hostnamePath", hostnamePath)
        .add("hostsPath", hostsPath)
        .add("name", name)
        .add("driver", driver)
        .add("execDriver", execDriver)
        .add("processLabel", processLabel)
        .add("mountLabel", mountLabel)
        .add("volumes", volumes)
        .add("volumesRW", volumesRW)
        .toString();
  }
}

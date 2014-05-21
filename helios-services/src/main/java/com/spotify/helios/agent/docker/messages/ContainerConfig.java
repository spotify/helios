package com.spotify.helios.agent.docker.messages;

import com.google.common.base.Objects;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;

@JsonAutoDetect(fieldVisibility = ANY, getterVisibility = NONE, setterVisibility = NONE)
public class ContainerConfig {

  @JsonProperty("Hostname") private String hostname;
  @JsonProperty("Domainname") private String domainname;
  @JsonProperty("User") private String user;
  @JsonProperty("Memory") private long memory;
  @JsonProperty("MemorySwap") private long memorySwap;
  @JsonProperty("CpuShares") private long cpuShares;
  @JsonProperty("Cpuset") private String cpuset;
  @JsonProperty("AttachStdin") private boolean attachStdin;
  @JsonProperty("AttachStdout") private boolean attachStdout;
  @JsonProperty("AttachStderr") private boolean attachStderr;
  @JsonProperty("PortSpecs") private List<String> portSpecs;
  @JsonProperty("ExposedPorts") private Set<String> exposedPorts;
  @JsonProperty("Tty") private boolean tty;
  @JsonProperty("OpenStdin") private boolean openStdin;
  @JsonProperty("StdinOnce") private boolean stdinOnce;
  @JsonProperty("Env") private List<String> env;
  @JsonProperty("Cmd") private List<String> cmd;
  @JsonProperty("Image") private String image;
  @JsonProperty("Volumes") private Set<String> volumes;
  @JsonProperty("WorkingDir") private String workingDir;
  @JsonProperty("Entrypoint") private List<String> entrypoint;
  @JsonProperty("NetworkDisabled") private boolean networkDisabled;
  @JsonProperty("OnBuild") private List<String> onBuild;

  public String hostname() {
    return hostname;
  }

  public void hostname(final String hostname) {
    this.hostname = hostname;
  }

  public String domainname() {
    return domainname;
  }

  public void domainname(final String domainname) {
    this.domainname = domainname;
  }

  public String user() {
    return user;
  }

  public void user(final String user) {
    this.user = user;
  }

  public long memory() {
    return memory;
  }

  public void memory(final long memory) {
    this.memory = memory;
  }

  public long memorySwap() {
    return memorySwap;
  }

  public void memorySwap(final long memorySwap) {
    this.memorySwap = memorySwap;
  }

  public long cpuShares() {
    return cpuShares;
  }

  public void cpuShares(final long cpuShares) {
    this.cpuShares = cpuShares;
  }

  public String cpuset() {
    return cpuset;
  }

  public void cpuset(final String cpuset) {
    this.cpuset = cpuset;
  }

  public boolean attachStdin() {
    return attachStdin;
  }

  public void attachStdin(final boolean attachStdin) {
    this.attachStdin = attachStdin;
  }

  public boolean attachStdout() {
    return attachStdout;
  }

  public void attachStdout(final boolean attachStdout) {
    this.attachStdout = attachStdout;
  }

  public boolean attachStderr() {
    return attachStderr;
  }

  public void attachStderr(final boolean attachStderr) {
    this.attachStderr = attachStderr;
  }

  public List<String> portSpecs() {
    return portSpecs;
  }

  public void portSpecs(final List<String> portSpecs) {
    this.portSpecs = portSpecs;
  }

  public Set<String> exposedPorts() {
    return exposedPorts;
  }

  public void exposedPorts(final Set<String> exposedPorts) {
    this.exposedPorts = exposedPorts;
  }

  public boolean tty() {
    return tty;
  }

  public void tty(final boolean tty) {
    this.tty = tty;
  }

  public boolean openStdin() {
    return openStdin;
  }

  public void openStdin(final boolean openStdin) {
    this.openStdin = openStdin;
  }

  public boolean stdinOnce() {
    return stdinOnce;
  }

  public void stdinOnce(final boolean stdinOnce) {
    this.stdinOnce = stdinOnce;
  }

  public List<String> env() {
    return env;
  }

  public void env(final List<String> env) {
    this.env = env;
  }

  public List<String> cmd() {
    return cmd;
  }

  public void cmd(final List<String> cmd) {
    this.cmd = cmd;
  }

  public String image() {
    return image;
  }

  public void image(final String image) {
    this.image = image;
  }

  public Set<String> volumes() {
    return volumes;
  }

  public void volumes(final Set<String> volumes) {
    this.volumes = volumes;
  }

  public String workingDir() {
    return workingDir;
  }

  public void workingDir(final String workingDir) {
    this.workingDir = workingDir;
  }

  public List<String> entrypoint() {
    return entrypoint;
  }

  public void entrypoint(final List<String> entrypoint) {
    this.entrypoint = entrypoint;
  }

  public boolean networkDisabled() {
    return networkDisabled;
  }

  public void networkDisabled(final boolean networkDisabled) {
    this.networkDisabled = networkDisabled;
  }

  public List<String> onBuild() {
    return onBuild;
  }

  public void onBuild(final List<String> onBuild) {
    this.onBuild = onBuild;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ContainerConfig that = (ContainerConfig) o;

    if (attachStderr != that.attachStderr) {
      return false;
    }
    if (attachStdin != that.attachStdin) {
      return false;
    }
    if (attachStdout != that.attachStdout) {
      return false;
    }
    if (cpuShares != that.cpuShares) {
      return false;
    }
    if (memory != that.memory) {
      return false;
    }
    if (memorySwap != that.memorySwap) {
      return false;
    }
    if (networkDisabled != that.networkDisabled) {
      return false;
    }
    if (openStdin != that.openStdin) {
      return false;
    }
    if (stdinOnce != that.stdinOnce) {
      return false;
    }
    if (tty != that.tty) {
      return false;
    }
    if (cmd != null ? !cmd.equals(that.cmd) : that.cmd != null) {
      return false;
    }
    if (cpuset != null ? !cpuset.equals(that.cpuset) : that.cpuset != null) {
      return false;
    }
    if (domainname != null ? !domainname.equals(that.domainname) : that.domainname != null) {
      return false;
    }
    if (entrypoint != null ? !entrypoint.equals(that.entrypoint) : that.entrypoint != null) {
      return false;
    }
    if (env != null ? !env.equals(that.env) : that.env != null) {
      return false;
    }
    if (exposedPorts != null ? !exposedPorts.equals(that.exposedPorts)
                             : that.exposedPorts != null) {
      return false;
    }
    if (hostname != null ? !hostname.equals(that.hostname) : that.hostname != null) {
      return false;
    }
    if (image != null ? !image.equals(that.image) : that.image != null) {
      return false;
    }
    if (onBuild != null ? !onBuild.equals(that.onBuild) : that.onBuild != null) {
      return false;
    }
    if (portSpecs != null ? !portSpecs.equals(that.portSpecs) : that.portSpecs != null) {
      return false;
    }
    if (user != null ? !user.equals(that.user) : that.user != null) {
      return false;
    }
    if (volumes != null ? !volumes.equals(that.volumes) : that.volumes != null) {
      return false;
    }
    if (workingDir != null ? !workingDir.equals(that.workingDir) : that.workingDir != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = hostname != null ? hostname.hashCode() : 0;
    result = 31 * result + (domainname != null ? domainname.hashCode() : 0);
    result = 31 * result + (user != null ? user.hashCode() : 0);
    result = 31 * result + (int) (memory ^ (memory >>> 32));
    result = 31 * result + (int) (memorySwap ^ (memorySwap >>> 32));
    result = 31 * result + (int) (cpuShares ^ (cpuShares >>> 32));
    result = 31 * result + (cpuset != null ? cpuset.hashCode() : 0);
    result = 31 * result + (attachStdin ? 1 : 0);
    result = 31 * result + (attachStdout ? 1 : 0);
    result = 31 * result + (attachStderr ? 1 : 0);
    result = 31 * result + (portSpecs != null ? portSpecs.hashCode() : 0);
    result = 31 * result + (exposedPorts != null ? exposedPorts.hashCode() : 0);
    result = 31 * result + (tty ? 1 : 0);
    result = 31 * result + (openStdin ? 1 : 0);
    result = 31 * result + (stdinOnce ? 1 : 0);
    result = 31 * result + (env != null ? env.hashCode() : 0);
    result = 31 * result + (cmd != null ? cmd.hashCode() : 0);
    result = 31 * result + (image != null ? image.hashCode() : 0);
    result = 31 * result + (volumes != null ? volumes.hashCode() : 0);
    result = 31 * result + (workingDir != null ? workingDir.hashCode() : 0);
    result = 31 * result + (entrypoint != null ? entrypoint.hashCode() : 0);
    result = 31 * result + (networkDisabled ? 1 : 0);
    result = 31 * result + (onBuild != null ? onBuild.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("hostname", hostname)
        .add("domainname", domainname)
        .add("user", user)
        .add("memory", memory)
        .add("memorySwap", memorySwap)
        .add("cpuShares", cpuShares)
        .add("cpuset", cpuset)
        .add("attachStdin", attachStdin)
        .add("attachStdout", attachStdout)
        .add("attachStderr", attachStderr)
        .add("portSpecs", portSpecs)
        .add("exposedPorts", exposedPorts)
        .add("tty", tty)
        .add("openStdin", openStdin)
        .add("stdinOnce", stdinOnce)
        .add("env", env)
        .add("cmd", cmd)
        .add("image", image)
        .add("volumes", volumes)
        .add("workingDir", workingDir)
        .add("entrypoint", entrypoint)
        .add("networkDisabled", networkDisabled)
        .add("onBuild", onBuild)
        .toString();
  }
}

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

/**
 * Structure representing the version and system details of the Docker daemon running on the
 * Agent.
 *
 * <p>A typical JSON representation of this might be:
 * <pre>
 * {
 *   "apiVersion" : "1.12",
 *   "arch" : "amd64",
 *   "gitCommit" : "688b5cf-dirty",
 *   "goVersion" : "go1.2.1",
 *   "kernelVersion" : "3.13.0-19-generic",
 *   "os" : "linux",
 *   "version" : "1.0.0"
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DockerVersion {

  private final String apiVersion;
  private final String arch;
  private final String gitCommit;
  private final String goVersion;
  private final String kernelVersion;
  private final String os;
  private final String version;

  /**
   * @param apiVersion    The Docker api version supported.
   * @param arch          The architecture of the machine.
   * @param gitCommit     The git commit from which Docker was built.
   * @param goVersion     The version of the Go compiler used to build Docker.
   * @param kernelVersion The kernel version of the Agent.
   * @param os            The operating system of the Agent.
   * @param version       The Docker version.
   */
  public DockerVersion(@JsonProperty("apiVersion") final String apiVersion,
                       @JsonProperty("arch") final String arch,
                       @JsonProperty("gitCommit") final String gitCommit,
                       @JsonProperty("goVersion") final String goVersion,
                       @JsonProperty("kernelVersion") final String kernelVersion,
                       @JsonProperty("os") final String os,
                       @JsonProperty("version") final String version) {
    this.apiVersion = apiVersion;
    this.arch = arch;
    this.gitCommit = gitCommit;
    this.goVersion = goVersion;
    this.kernelVersion = kernelVersion;
    this.os = os;
    this.version = version;
  }

  private DockerVersion(final Builder builder) {
    this.apiVersion = builder.apiVersion;
    this.arch = builder.arch;
    this.gitCommit = builder.gitCommit;
    this.goVersion = builder.goVersion;
    this.kernelVersion = builder.kernelVersion;
    this.os = builder.os;
    this.version = builder.version;
  }

  public String getApiVersion() {
    return apiVersion;
  }

  public String getArch() {
    return arch;
  }

  public String getGitCommit() {
    return gitCommit;
  }

  public String getGoVersion() {
    return goVersion;
  }

  public String getKernelVersion() {
    return kernelVersion;
  }

  public String getOs() {
    return os;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final DockerVersion that = (DockerVersion) obj;

    if (apiVersion != null ? !apiVersion.equals(that.apiVersion) : that.apiVersion != null) {
      return false;
    }
    if (arch != null ? !arch.equals(that.arch) : that.arch != null) {
      return false;
    }
    if (gitCommit != null ? !gitCommit.equals(that.gitCommit) : that.gitCommit != null) {
      return false;
    }
    if (goVersion != null ? !goVersion.equals(that.goVersion) : that.goVersion != null) {
      return false;
    }
    if (kernelVersion != null ? !kernelVersion.equals(that.kernelVersion)
                              : that.kernelVersion != null) {
      return false;
    }
    if (os != null ? !os.equals(that.os) : that.os != null) {
      return false;
    }
    if (version != null ? !version.equals(that.version) : that.version != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = apiVersion != null ? apiVersion.hashCode() : 0;
    result = 31 * result + (arch != null ? arch.hashCode() : 0);
    result = 31 * result + (gitCommit != null ? gitCommit.hashCode() : 0);
    result = 31 * result + (goVersion != null ? goVersion.hashCode() : 0);
    result = 31 * result + (kernelVersion != null ? kernelVersion.hashCode() : 0);
    result = 31 * result + (os != null ? os.hashCode() : 0);
    result = 31 * result + (version != null ? version.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "DockerVersion{"
           + "apiVersion='" + apiVersion + '\''
           + ", arch='" + arch + '\''
           + ", gitCommit='" + gitCommit + '\''
           + ", goVersion='" + goVersion + '\''
           + ", kernelVersion='" + kernelVersion + '\''
           + ", os='" + os + '\''
           + ", version='" + version + '\''
           + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Builder() {
    }

    private String apiVersion;
    private String arch;
    private String gitCommit;
    private String goVersion;
    private String kernelVersion;
    private String os;
    private String version;

    public Builder apiVersion(final String apiVersion) {
      this.apiVersion = apiVersion;
      return this;
    }

    public Builder arch(final String arch) {
      this.arch = arch;
      return this;
    }

    public Builder gitCommit(final String gitCommit) {
      this.gitCommit = gitCommit;
      return this;
    }

    public Builder goVersion(final String goVersion) {
      this.goVersion = goVersion;
      return this;
    }

    public Builder kernelVersion(final String kernelVersion) {
      this.kernelVersion = kernelVersion;
      return this;
    }

    public Builder os(final String os) {
      this.os = os;
      return this;
    }

    public Builder version(final String version) {
      this.version = version;
      return this;
    }

    public DockerVersion build() {
      return new DockerVersion(this);
    }
  }
}

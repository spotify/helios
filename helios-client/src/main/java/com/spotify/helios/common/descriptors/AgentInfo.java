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
import java.util.List;

/**
 * Information about the Helios Agent (aka "host").
 *
 * <p>A typical JSON representation might look like this:
 * <pre>
 * {
 *   "inputArguments" : [ "-Xmx256m", "-Dcom.sun.management.jmxremote.port=9203",
 *       "-Dcom.sun.management.jmxremote.ssl=false",
 *       "-Dcom.sun.management.jmxremote.authenticate=false", "-Djava.net.preferIPv4Stack=true",
 *       "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006" ],
 *   "name" : "agent-name",
 *   "specName" : "Java Virtual Machine Specification",
 *   "specVendor" : "Oracle Corporation",
 *   "specVersion" : "1.7",
 *   "startTime" : 1410308461448,
 *   "uptime" : 231779308,
 *   "version" : "0.8.10",
 *   "vmName" : "Java HotSpot(TM) 64-Bit Server VM",
 *   "vmVendor" : "Oracle Corporation",
 *   "vmVersion" : "24.45-b08"
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AgentInfo extends Descriptor {
  private final String name;
  private final String vmName;
  private final String vmVendor;
  private final String vmVersion;
  private final String specName;
  private final String specVendor;
  private final String specVersion;
  private final List<String> inputArguments;
  private final long uptime;
  private final long startTime;
  private final String version;

  /**
   * Constructor.
   *
   * @param name           The name of the agent.
   * @param vmName         The name of the JVM.
   * @param vmVendor       The vendor of the JVM.
   * @param vmVersion      The version of the JVM.
   * @param specName       The specification of the JVM.
   * @param specVendor     The specification vendor of the JVM.
   * @param specVersion    The specification version of the JVM.
   * @param inputArguments JVM arguments passed when starting the agent.
   * @param uptime         How long the Agent has been running.
   * @param startTime      The clock time at which the Agent started.
   * @param version        The Helios version of the running Agent.
   */
  public AgentInfo(@JsonProperty("name") final String name,
                   @JsonProperty("vmName") final String vmName,
                   @JsonProperty("vmVendor") final String vmVendor,
                   @JsonProperty("vmVersion") final String vmVersion,
                   @JsonProperty("specName") final String specName,
                   @JsonProperty("specVendor") final String specVendor,
                   @JsonProperty("specVersion") final String specVersion,
                   @JsonProperty("inputArguments") final List<String> inputArguments,
                   @JsonProperty("uptime") final long uptime,
                   @JsonProperty("startTime") final long startTime,
                   @JsonProperty("version") final String version) {
    this.name = name;
    this.vmName = vmName;
    this.vmVendor = vmVendor;
    this.vmVersion = vmVersion;
    this.specName = specName;
    this.specVendor = specVendor;
    this.specVersion = specVersion;
    this.inputArguments = inputArguments;
    this.uptime = uptime;
    this.startTime = startTime;
    this.version = version;
  }

  public AgentInfo(final Builder builder) {
    this.name = builder.name;
    this.vmName = builder.vmName;
    this.vmVendor = builder.vmVendor;
    this.vmVersion = builder.vmVersion;
    this.specName = builder.specName;
    this.specVendor = builder.specVendor;
    this.specVersion = builder.specVersion;
    this.inputArguments = builder.inputArguments;
    this.uptime = builder.uptime;
    this.startTime = builder.startTime;
    this.version = builder.version;
  }

  public String getName() {
    return name;
  }

  public String getVmName() {
    return vmName;
  }

  public String getVmVendor() {
    return vmVendor;
  }

  public String getVmVersion() {
    return vmVersion;
  }

  public String getSpecName() {
    return specName;
  }

  public String getSpecVendor() {
    return specVendor;
  }

  public String getSpecVersion() {
    return specVersion;
  }

  public List<String> getInputArguments() {
    return inputArguments;
  }

  public long getUptime() {
    return uptime;
  }

  public long getStartTime() {
    return startTime;
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

    final AgentInfo agentInfo = (AgentInfo) obj;

    if (startTime != agentInfo.startTime) {
      return false;
    }
    if (uptime != agentInfo.uptime) {
      return false;
    }
    if (inputArguments != null ? !inputArguments.equals(agentInfo.inputArguments)
                               : agentInfo.inputArguments != null) {
      return false;
    }
    if (name != null ? !name.equals(agentInfo.name) : agentInfo.name != null) {
      return false;
    }
    if (specName != null ? !specName.equals(agentInfo.specName) : agentInfo.specName != null) {
      return false;
    }
    if (specVendor != null ? !specVendor.equals(agentInfo.specVendor)
                           : agentInfo.specVendor != null) {
      return false;
    }
    if (specVersion != null ? !specVersion.equals(agentInfo.specVersion)
                            : agentInfo.specVersion != null) {
      return false;
    }
    if (version != null ? !version.equals(agentInfo.version) : agentInfo.version != null) {
      return false;
    }
    if (vmName != null ? !vmName.equals(agentInfo.vmName) : agentInfo.vmName != null) {
      return false;
    }
    if (vmVendor != null ? !vmVendor.equals(agentInfo.vmVendor) : agentInfo.vmVendor != null) {
      return false;
    }
    if (vmVersion != null ? !vmVersion.equals(agentInfo.vmVersion) : agentInfo.vmVersion != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (vmName != null ? vmName.hashCode() : 0);
    result = 31 * result + (vmVendor != null ? vmVendor.hashCode() : 0);
    result = 31 * result + (vmVersion != null ? vmVersion.hashCode() : 0);
    result = 31 * result + (specName != null ? specName.hashCode() : 0);
    result = 31 * result + (specVendor != null ? specVendor.hashCode() : 0);
    result = 31 * result + (specVersion != null ? specVersion.hashCode() : 0);
    result = 31 * result + (inputArguments != null ? inputArguments.hashCode() : 0);
    result = 31 * result + (int) (uptime ^ (uptime >>> 32));
    result = 31 * result + (int) (startTime ^ (startTime >>> 32));
    result = 31 * result + (version != null ? version.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "AgentInfo{"
           + "name='" + name + '\''
           + ", vmName='" + vmName + '\''
           + ", vmVendor='" + vmVendor + '\''
           + ", vmVersion='" + vmVersion + '\''
           + ", specName='" + specName + '\''
           + ", specVendor='" + specVendor + '\''
           + ", specVersion='" + specVersion + '\''
           + ", inputArguments=" + inputArguments
           + ", uptime=" + uptime
           + ", startTime=" + startTime
           + '}';
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String name;
    private String vmName;
    private String vmVendor;
    private String vmVersion;
    private String specName;
    private String specVendor;
    private String specVersion;
    private List<String> inputArguments;
    private long uptime;
    private long startTime;
    private String version;

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setVmName(final String vmName) {
      this.vmName = vmName;
      return this;
    }

    public Builder setVmVendor(final String vmVendor) {
      this.vmVendor = vmVendor;
      return this;
    }

    public Builder setVmVersion(final String vmVersion) {
      this.vmVersion = vmVersion;
      return this;
    }

    public Builder setSpecName(final String specName) {
      this.specName = specName;
      return this;
    }

    public Builder setSpecVendor(final String specVendor) {
      this.specVendor = specVendor;
      return this;
    }

    public Builder setSpecVersion(final String specVersion) {
      this.specVersion = specVersion;
      return this;
    }

    public Builder setInputArguments(final List<String> inputArguments) {
      this.inputArguments = inputArguments;
      return this;
    }

    public Builder setUptime(final long uptime) {
      this.uptime = uptime;
      return this;
    }

    public Builder setStartTime(final long startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder setVersion(final String version) {
      this.version = version;
      return this;
    }

    public AgentInfo build() {
      return new AgentInfo(this);
    }
  }
}

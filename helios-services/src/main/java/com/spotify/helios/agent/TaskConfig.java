/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.agent;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.common.descriptors.HealthCheck;
import com.spotify.helios.common.descriptors.HttpHealthCheck;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.Resources;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePortParameters;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TcpHealthCheck;
import com.spotify.helios.serviceregistration.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

/**
 * Provides docker container configuration for running a task.
 */
public class TaskConfig {

  private static final Logger log = LoggerFactory.getLogger(TaskConfig.class);

  private static final Pattern CONTAINER_NAME_FORBIDDEN = Pattern.compile("[^a-zA-Z0-9_-]");

  private final String host;
  private final Map<String, Integer> ports;
  private final Job job;
  private final Map<String, String> envVars;
  private final List<ContainerDecorator> containerDecorators;
  private final String namespace;
  private final String defaultRegistrationDomain;
  private final List<String> dns;
  private final List<String> securityOpt;
  private final String networkMode;

  private TaskConfig(final Builder builder) {
    this.host = checkNotNull(builder.host, "host");
    this.ports = checkNotNull(builder.ports, "ports");
    this.job = checkNotNull(builder.job, "job");
    this.envVars = checkNotNull(builder.envVars, "envVars");
    this.containerDecorators = checkNotNull(builder.containerDecorators, "containerDecorators");
    this.namespace = checkNotNull(builder.namespace, "namespace");
    this.defaultRegistrationDomain = checkNotNull(builder.defaultRegistrationDomain,
        "defaultRegistrationDomain");
    this.dns = checkNotNull(builder.dns, "dns");
    this.securityOpt = checkNotNull(builder.securityOpt, "securityOpt");
    this.networkMode = checkNotNull(builder.networkMode, "networkMode");
  }

  /**
   * Generate a random container name.
   * @return The random container name.
   */
  public String containerName() {
    final String shortId = job.getId().toShortString();
    final String escaped = CONTAINER_NAME_FORBIDDEN.matcher(shortId).replaceAll("_");
    final String random = Integer.toHexString(new SecureRandom().nextInt());
    return namespace + "-" + escaped + "_" + random;
  }

  /**
   * Create docker container configuration for a job.
   * @param imageInfo The ImageInfo object.
   * @return The ContainerConfig object.
   */
  public ContainerConfig containerConfig(final ImageInfo imageInfo) {
    final ContainerConfig.Builder builder = ContainerConfig.builder();

    builder.image(job.getImage());
    builder.cmd(job.getCommand());
    builder.hostname(job.getHostname());
    builder.env(containerEnvStrings());
    builder.exposedPorts(containerExposedPorts());
    builder.volumes(volumes());

    final Resources resources = job.getResources();
    if (resources != null) {
      builder.memory(resources.getMemory());
      builder.memorySwap(resources.getMemorySwap());
      builder.cpuset(resources.getCpuset());
      builder.cpuShares(resources.getCpuShares());
    }

    for (final ContainerDecorator decorator : containerDecorators) {
      decorator.decorateContainerConfig(job, imageInfo, builder);
    }

    return builder.build();
  }

  /**
   * Get final port mappings using allocated ports.
   * @return The port mapping.
   */
  public Map<String, PortMapping> ports() {
    final ImmutableMap.Builder<String, PortMapping> builder = ImmutableMap.builder();
    for (final Map.Entry<String, PortMapping> e : job.getPorts().entrySet()) {
      final PortMapping mapping = e.getValue();
      builder.put(e.getKey(), mapping.hasExternalPort()
                              ? mapping
                              : mapping.withExternalPort(checkNotNull(ports.get(e.getKey()))));
    }
    return builder.build();
  }

  /**
   * Get environment variables for the container.
   * @return The environment variables.
   */
  public Map<String, String> containerEnv() {
    final Map<String, String> env = Maps.newHashMap(envVars);

    // Put in variables that tell the container where it's exposed
    for (Entry<String, Integer> entry : ports.entrySet()) {
      env.put("HELIOS_PORT_" + entry.getKey(), host + ":" + entry.getValue());
    }
    // Job environment variables take precedence.
    env.putAll(job.getEnv());
    return env;
  }

  public ServiceRegistration registration()
      throws InterruptedException {
    final ServiceRegistration.Builder builder = ServiceRegistration.newBuilder();

    for (final Map.Entry<ServiceEndpoint, ServicePorts> entry :
        job.getRegistration().entrySet()) {
      final ServiceEndpoint registration = entry.getKey();
      final ServicePorts servicePorts = entry.getValue();
      for (final Entry<String, ServicePortParameters> portEntry :
          servicePorts.getPorts().entrySet()) {
        final String portName = portEntry.getKey();
        final ServicePortParameters portParameters = portEntry.getValue();
        final PortMapping mapping = job.getPorts().get(portName);
        if (mapping == null) {
          log.error("no '{}' port mapped for registration: '{}'", portName, registration);
          continue;
        }
        final Integer externalPort;
        if (mapping.getExternalPort() != null) {
          // Use the statically assigned port if one is specified
          externalPort = mapping.getExternalPort();
        } else {
          // Otherwise use the dynamically allocated port
          externalPort = ports.get(portName);
        }
        if (externalPort == null) {
          log.error("no external '{}' port for registration: '{}'", portName, registration);
          continue;
        }

        builder.endpoint(registration.getName(), registration.getProtocol(), externalPort,
            fullyQualifiedRegistrationDomain(), host, portParameters.getTags(),
            endpointHealthCheck(portName));
      }
    }

    return builder.build();
  }

  /**
   * Get endpoint health check for a given port
   * @param portName The port name
   * @return An EndpointHealthCheck or null if no check exists
   */
  private ServiceRegistration.EndpointHealthCheck endpointHealthCheck(String portName) {
    if (healthCheck() instanceof HttpHealthCheck) {
      HttpHealthCheck httpHealthCheck = (HttpHealthCheck) healthCheck();
      if (portName.equals(httpHealthCheck.getPort())) {
        return ServiceRegistration.EndpointHealthCheck.newHttpCheck(httpHealthCheck.getPath());
      }
    } else if (healthCheck() instanceof TcpHealthCheck) {
      if (portName.equals(((TcpHealthCheck) healthCheck()).getPort())) {
        return ServiceRegistration.EndpointHealthCheck.newTcpCheck();
      }
    }
    return null;
  }

  public HealthCheck healthCheck() {
    return job.getHealthCheck();
  }

  /**
   * Given the registration domain in the job, and the default registration domain for the agent,
   * figure out what domain we should actually register the job in.
   * @return The full registration domain.
   */
  private String fullyQualifiedRegistrationDomain() {
    if (job.getRegistrationDomain().endsWith(".")) {
      return job.getRegistrationDomain();
    } else if ("".equals(job.getRegistrationDomain())) {
      return defaultRegistrationDomain;
    } else {
      return job.getRegistrationDomain() + "." + defaultRegistrationDomain;
    }
  }

  /**
   * Create container port exposure configuration for a job.
   * @return The exposed ports.
   */
  private Set<String> containerExposedPorts() {
    final Set<String> ports = Sets.newHashSet();
    for (final Map.Entry<String, PortMapping> entry : job.getPorts().entrySet()) {
      final PortMapping mapping = entry.getValue();
      ports.add(containerPort(mapping.getInternalPort(), mapping.getProtocol()));
    }
    return ports;
  }

  /**
   * Compute docker container environment variables.
   * @return The container environment variables.
   */
  private List<String> containerEnvStrings() {
    final Map<String, String> env = containerEnv();
    final List<String> envList = Lists.newArrayList();
    for (final Map.Entry<String, String> entry : env.entrySet()) {
      envList.add(entry.getKey() + '=' + entry.getValue());
    }
    return envList;
  }

  /**
   * Create a port binding configuration for the job.
   * @return The port bindings.
   */
  private Map<String, List<PortBinding>> portBindings() {
    final Map<String, List<PortBinding>> bindings = Maps.newHashMap();
    for (final Map.Entry<String, PortMapping> e : job.getPorts().entrySet()) {
      final PortMapping mapping = e.getValue();
      final PortBinding binding = new PortBinding();
      final Integer externalPort = mapping.getExternalPort();
      if (externalPort == null) {
        binding.hostPort(ports.get(e.getKey()).toString());
      } else {
        binding.hostPort(externalPort.toString());
      }
      final String entry = containerPort(mapping.getInternalPort(), mapping.getProtocol());
      bindings.put(entry, asList(binding));
    }
    return bindings;
  }

  /**
   * Create a container host configuration for the job.
   * @return The host configuration.
   */
  public HostConfig hostConfig() {
    final HostConfig.Builder builder = HostConfig.builder()
        .binds(binds())
        .portBindings(portBindings())
        .dns(dns)
        .securityOpt(securityOpt.toArray(new String[securityOpt.size()]))
        .networkMode(networkMode);

    for (final ContainerDecorator decorator : containerDecorators) {
      decorator.decorateHostConfig(builder);
    }

    return builder.build();
  }

  /**
   * Get container volumes.
   * @return A set of container volumes.
   */
  private Set<String> volumes() {
    final ImmutableSet.Builder<String> volumes = ImmutableSet.builder();
    for (Map.Entry<String, String> entry : job.getVolumes().entrySet()) {
      final String path = entry.getKey();
      final String source = entry.getValue();
      if (Strings.isNullOrEmpty(source)) {
        volumes.add(path);
      }
    }
    return volumes.build();
  }

  /**
   * Get container bind mount volumes.
   * @return A list of container bind mount volumes.
   */
  private List<String> binds() {
    final ImmutableList.Builder<String> binds = ImmutableList.builder();
    for (Map.Entry<String, String> entry : job.getVolumes().entrySet()) {
      final String path = entry.getKey();
      final String source = entry.getValue();
      if (Strings.isNullOrEmpty(source)) {
        continue;
      }
      binds.add(source + ":" + path);
    }
    return binds.build();
  }

  /**
   * Create a docker port exposure/mapping entry.
   * @param port The port.
   * @param protocol The protocol.
   * @return A string representing the port and protocol.
   */
  private String containerPort(final int port, final String protocol) {
    return port + "/" + protocol;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String containerImage() {
    return job.getImage();
  }

  public String name() {
    return job.getId().toShortString();
  }

  public static class Builder {

    private Builder() {
    }

    private String host;
    private Job job;
    private Map<String, Integer> ports = Collections.emptyMap();
    private Map<String, String> envVars = Collections.emptyMap();
    private List<ContainerDecorator> containerDecorators = Lists.newArrayList();
    private String namespace;
    private String defaultRegistrationDomain = "";
    private List<String> dns = Collections.emptyList();
    private List<String> securityOpt = Collections.emptyList();
    private String networkMode = "";

    public Builder host(final String host) {
      this.host = host;
      return this;
    }

    public Builder job(final Job job) {
      this.job = job;
      return this;
    }

    public Builder defaultRegistrationDomain(final String domain) {
      this.defaultRegistrationDomain = checkNotNull(domain, "domain");
      return this;
    }

    public Builder ports(final Map<String, Integer> ports) {
      this.ports = ports;
      return this;
    }

    public Builder envVars(final Map<String, String> envVars) {
      this.envVars = envVars;
      return this;
    }

    public Builder containerDecorators(final List<ContainerDecorator> containerDecorators) {
      this.containerDecorators = containerDecorators;
      return this;
    }

    public Builder namespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    public Builder dns(final List<String> dns) {
      this.dns = dns;
      return this;
    }

    public Builder securityOpt(final List<String> securityOpt) {
      this.securityOpt = securityOpt;
      return this;
    }

    public Builder networkMode(final String networkMode) {
      this.networkMode = networkMode;
      return this;
    }

    public TaskConfig build() {
      return new TaskConfig(this);
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("job", job)
        .add("host", host)
        .add("ports", ports)
        .add("envVars", envVars)
        .add("containerDecorators", containerDecorators)
        .add("defaultRegistrationDomain", defaultRegistrationDomain)
        .add("securityOpt", securityOpt)
        .add("dns", dns)
        .add("networkMode", networkMode)
        .toString();
  }
}


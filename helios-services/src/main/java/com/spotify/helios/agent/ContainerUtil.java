package com.spotify.helios.agent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.spotify.helios.agent.docker.messages.ContainerConfig;
import com.spotify.helios.agent.docker.messages.HostConfig;
import com.spotify.helios.agent.docker.messages.ImageInfo;
import com.spotify.helios.agent.docker.messages.PortBinding;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;

import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

/**
 * The miscellaneous bag of utility functions to make/consume things to/from the docker
 * client.
 */
public class ContainerUtil {

  private static final Pattern CONTAINER_NAME_FORBIDDEN = Pattern.compile("[^a-zA-Z0-9_-]");
  private static final int HOST_NAME_MAX = 64;

  private final String host;
  private final Map<String, Integer> ports;
  private final Job job;
  private final Map<String, String> envVars;
  private final ContainerDecorator containerDecorator;

  public ContainerUtil(final String host,
                       final Job job,
                       final Map<String, Integer> ports,
                       final Map<String, String> envVars,
                       final ContainerDecorator containerDecorator) {
    this.host = host;
    this.ports = ports;
    this.job = job;
    this.envVars = envVars;
    this.containerDecorator = containerDecorator;
  }

  /**
   * Generate a random container name.
   */
  public String containerName() {
    final String shortId = job.getId().toShortString();
    final String escaped = CONTAINER_NAME_FORBIDDEN.matcher(shortId).replaceAll("_");
    final String random = Integer.toHexString(new SecureRandom().nextInt());
    return escaped + "_" + random;
  }

  /**
   * Create docker container configuration for a job.
   */
  public ContainerConfig containerConfig(final ImageInfo imageInfo) {
    final ContainerConfig containerConfig = new ContainerConfig();
    containerConfig.image(job.getImage());
    containerConfig.cmd(job.getCommand());
    containerConfig.env(containerEnvStrings());
    containerConfig.exposedPorts(containerExposedPorts());
    containerConfig.hostname(containerHostname(job.getId().getName() + "_" +
                                               job.getId().getVersion()));
    containerConfig.domainname(host);
    containerDecorator.decorateContainerConfig(job, imageInfo, containerConfig);
    return containerConfig;
  }

  /**
   * Get final port mappings using allocated ports.
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
   */
  public Map<String, String> containerEnv() {
    final Map<String, String> env = Maps.newHashMap(envVars);
    // Job environment variables take precedence.
    env.putAll(job.getEnv());
    return env;
  }

  /**
   * Create container port exposure configuration for a job.
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
   * Generate a host name for the container
   */
  private String containerHostname(String name) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if ((c >= 'A' && c <= 'Z')
          || (c >= 'a' && c <= 'z')
          || (c >= '0' && c <= '9')) {
        sb.append(c);
      } else {
        sb.append('_');
      }
    }
    return truncate(sb.toString(), HOST_NAME_MAX);
  }

  /**
   * Compute docker container environment variables.
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
   */
  public HostConfig hostConfig() {
    final HostConfig hostConfig = new HostConfig();
    hostConfig.portBindings(portBindings());
    containerDecorator.decorateHostConfig(hostConfig);
    return hostConfig;
  }

  /**
   * Create a docker port exposure/mapping entry.
   */
  private String containerPort(final int port, final String protocol) {
    return port + "/" + protocol;
  }

  private static String truncate(final String s, final int len) {
    // TODO (dano): replace with guava 16+
    return s.substring(0, Math.min(len, s.length()));
  }
}


package com.spotify.helios.agent;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.PortBinding;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The miscellaneous bag of utility functions to make/consume things to/from the docker
 * client.
 */
public class ContainerUtil {
  public static class PullingException extends Exception {}

  private static final Logger log = LoggerFactory.getLogger(ContainerUtil.class);
  private static final int HOST_NAME_MAX = 64;
  private static final long PULL_POLL_TIMEOUT_SECONDS = 30;

  private final String host;
  private final Map<String, Integer> ports;
  private final Job job;
  private final Map<String, String> envVars;
  private final ListeningExecutorService pullStreamExecutor;

  public ContainerUtil(final String host,
                       final Job job,
                       final Map<String, Integer> ports,
                       final Map<String, String> envVars) {
    this.host = host;
    this.ports = ports;
    this.job = job;
    this.envVars = envVars;
    this.pullStreamExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
 }

  /**
   * Create docker container configuration for a job.
   */
  public ContainerConfig containerConfig(final Job descriptor) {
    final ContainerConfig containerConfig = new ContainerConfig();
    containerConfig.setImage(descriptor.getImage());
    final List<String> command = descriptor.getCommand();
    containerConfig.setCmd(command.toArray(new String[command.size()]));
    containerConfig.setEnv(containerEnv(descriptor));
    containerConfig.setExposedPorts(containerExposedPorts());
    containerConfig.setHostName(safeHostNameify(descriptor.getId().getName() + "_" +
                                                descriptor.getId().getVersion()));
    containerConfig.setDomainName(host);
    return containerConfig;
  }

  /**
   * Create container port exposure configuration for a job.
   */
  public Map<String, Void> containerExposedPorts() {
    final Map<String, Void> ports = Maps.newHashMap();
    for (final Map.Entry<String, PortMapping> entry : job.getPorts().entrySet()) {
      final PortMapping mapping = entry.getValue();
      ports.put(containerPort(mapping.getInternalPort(), mapping.getProtocol()), null);
    }
    return ports;
  }

  private String safeHostNameify(String name) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if ( (c >= 'A' && c <= 'Z')
           || (c >= 'a' && c <= 'z')
           || (c >= '0' && c <= '9')) {
        sb.append(c);
      } else {
        sb.append('_');
      }
    }
    return truncate(sb.toString(), HOST_NAME_MAX);
  }

  private static String truncate(final String s, final int len) {
    return s.substring(0, Math.min(len, s.length()));
  }

  /**
   * Compute docker container environment variables.
   */
  private String[] containerEnv(final Job descriptor) {
    final Map<String, String> env = getContainerEnvMap();

    final List<String> envList = Lists.newArrayList();
    for (final Map.Entry<String, String> entry : env.entrySet()) {
      envList.add(entry.getKey() + '=' + entry.getValue());
    }

    return envList.toArray(new String[envList.size()]);
  }

  public Map<String, String> getContainerEnvMap() {
    final Map<String, String> env = Maps.newHashMap(envVars);
    // Job environment variables take precedence.
    env.putAll(job.getEnv());
    return env;
  }

  /**
   * Create a port binding configuration for the job.
   */
  private Map<String, List<PortBinding>> portBindings() {
    final Map<String, List<PortBinding>> bindings = Maps.newHashMap();
    for (final Map.Entry<String, PortMapping> e : job.getPorts().entrySet()) {
      final PortMapping mapping = e.getValue();
      final PortBinding binding = new PortBinding();
      if (mapping.getExternalPort() == null) {
        binding.hostPort = ports.get(e.getKey()).toString();
      } else {
        binding.hostPort = mapping.getExternalPort().toString();
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
    hostConfig.portBindings = portBindings();
    return hostConfig;
  }

  /**
   * Create a docker port exposure/mapping entry.
   */
  private String containerPort(final int port, final String protocol) {
    return port + "/" + protocol;
  }

  public static String containerName(final JobId id) {
    final String random = Integer.toHexString(new SecureRandom().nextInt());
    return id.toShortString().replace(':', '_') + "_" + random;
  }

  public Map<String, PortMapping> parsePortBindings(final ContainerInspectResponse info) {
    if (info.networkSettings.ports == null) {
      return emptyMap();
    }
    return parsePortBindings(info.networkSettings.ports);
  }

  private Map<String, PortMapping> parsePortBindings(final Map<String, List<PortBinding>> ports) {
    final ImmutableMap.Builder<String, PortMapping> builder = ImmutableMap.builder();
    for (final Map.Entry<String, List<PortBinding>> e : ports.entrySet()) {
      final PortMapping mapping = parsePortBinding(e.getKey(), e.getValue());
      final String name = getPortNameForPortNumber(mapping.getInternalPort());
      if (name == null) {
        log.info("got internal port unknown to the job: {}", mapping.getInternalPort());
      } else if (mapping.getExternalPort() == null) {
        log.debug("unbound port: {}/{}", name, mapping.getInternalPort());
      } else {
        builder.put(name, mapping);
      }
    }
    return builder.build();
  }


  /**
   * Assumes port binding matches output of {@link #portBindings}
   */
  private PortMapping parsePortBinding(final String entry, final List<PortBinding> bindings) {
    final List<String> parts = Splitter.on('/').splitToList(entry);
    if (parts.size() != 2) {
      throw new IllegalArgumentException("Invalid port binding: " + entry);
    }

    final String protocol = parts.get(1);

    final int internalPort;
    try {
      internalPort = Integer.parseInt(parts.get(0));
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Invalid port binding: " + entry, ex);
    }

    if (bindings == null) {
      return PortMapping.of(internalPort);
    } else {
      if (bindings.size() != 1) {
        throw new IllegalArgumentException("Expected single binding, got " + bindings.size());
      }

      final PortBinding binding = bindings.get(0);
      final int externalPort;
      try {
        externalPort = Integer.parseInt(binding.hostPort);
      } catch (NumberFormatException e1) {
        throw new IllegalArgumentException("Invalid host port: " + binding.hostPort);
      }
      return PortMapping.of(internalPort, externalPort, protocol);
    }
  }

  private String getPortNameForPortNumber(final int internalPort) {
    for (final Entry<String, PortMapping> portMapping : job.getPorts().entrySet()) {
      if (portMapping.getValue().getInternalPort() == internalPort) {
        log.info("found mapping for internal port {} {} -> {}",
            internalPort,
            portMapping.getValue().getInternalPort(),
            portMapping.getKey());
        return portMapping.getKey();
      }
    }
    return null;
  }

  public void tailPull(final String image, final InputStream stream)
      throws ImagePullFailedException, ImageMissingException, PullingException {

    final MappingIterator<Map<String, Object>> messages;
    try {
      messages = Json.readValues(stream, new TypeReference<Map<String, Object>>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // TODO (dano): this can block forever and seems to be impossible to abort by closing the client
    while (true) {
      if (!hasNext(messages)) {
        break;
      }
      Map<String, Object> message = getNext(messages);
      final Object error = message.get("error");
      if (error != null) {
        if (error.toString().contains("404")) {
          throw new ImageMissingException(message.toString());
        } else {
          throw new ImagePullFailedException(message.toString());
        }
      }
      log.info("pull {}: {}", image, message);
    }
  }

  private Map<String, Object> getNext(final MappingIterator<Map<String, Object>> messages)
      throws PullingException {
    final ListenableFuture<Map<String, Object>> future = pullStreamExecutor.submit(
      new Callable<Map<String, Object>>() {
        @Override
        public Map<String, Object> call() throws Exception {
          return messages.next();
        }
      });
    try {
      return Futures.get(future, PULL_POLL_TIMEOUT_SECONDS, SECONDS, PullingException.class);
    } catch (PullingException e) {
      future.cancel(true);
      throw e;
    }
  }

  private boolean hasNext(final MappingIterator<Map<String, Object>> messages)
      throws PullingException {
    final ListenableFuture<Boolean> future = pullStreamExecutor.submit(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return messages.hasNext();
      }
    });
    try {
      return Futures.get(future, PULL_POLL_TIMEOUT_SECONDS, SECONDS, PullingException.class);
    } catch (PullingException e) {
      future.cancel(true);
      throw e;
    }
  }
}


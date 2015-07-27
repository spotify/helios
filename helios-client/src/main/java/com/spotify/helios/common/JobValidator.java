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

package com.spotify.helios.common;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import com.spotify.helios.common.descriptors.ExecHealthCheck;
import com.spotify.helios.common.descriptors.HealthCheck;
import com.spotify.helios.common.descriptors.HttpHealthCheck;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TcpHealthCheck;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.regex.Pattern.compile;

public class JobValidator {

  public static final Pattern NAME_VERSION_PATTERN = Pattern.compile("[0-9a-zA-Z-_.]+");

  public static final Pattern DOMAIN_PATTERN =
      Pattern.compile("^(?:(?:[a-zA-Z0-9]|(?:[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9]))" +
                      "(\\.(?:[a-zA-Z0-9]|(?:[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])))*)\\.?$");

  public static final Pattern IPV4_PATTERN =
      Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");

  public static final Pattern NAMESPACE_PATTERN = Pattern.compile("^([a-z0-9_]{4,30})$");
  public static final Pattern REPO_PATTERN = Pattern.compile("^([a-z0-9-_.]+)$");
  public static final Pattern DIGIT_PERIOD = Pattern.compile("^[0-9.]+$");

  public static final Pattern PORT_MAPPING_PROTO_PATTERN = compile("(tcp|udp)");
  public static final Pattern PORT_MAPPING_NAME_PATTERN = compile("\\S+");
  public static final Pattern REGISTRATION_NAME_PATTERN = compile("[_\\-\\w]+");

  public Set<String> validate(final Job job) {
    final Set<String> errors = Sets.newHashSet();

    errors.addAll(validateJobId(job));
    errors.addAll(validateJobImage(job.getImage()));

    // Check that there's not external port collision
    final Set<Integer> externalPorts = Sets.newHashSet();
    for (final PortMapping mapping : job.getPorts().values()) {
      Integer externalMappedPort = mapping.getExternalPort();
      if (externalPorts.contains(externalMappedPort) && externalMappedPort != null) {
        errors.add(format("Duplicate external port mapping: %s", externalMappedPort));
      }
      externalPorts.add(externalMappedPort);
    }

    // Verify port mappings
    for (final Map.Entry<String, PortMapping> entry : job.getPorts().entrySet()) {
      final String name = entry.getKey();
      final PortMapping mapping = entry.getValue();
      if (!PORT_MAPPING_PROTO_PATTERN.matcher(mapping.getProtocol()).matches()) {
        errors.add(format("Invalid port mapping protocol: %s", mapping.getProtocol()));
      }
      if (!legalPort(mapping.getInternalPort())) {
        errors.add(format("Invalid internal port: %d", mapping.getInternalPort()));
      }
      if (mapping.getExternalPort() != null && !legalPort(mapping.getExternalPort())) {
        errors.add(format("Invalid external port: %d", mapping.getExternalPort()));
      }
      if (!PORT_MAPPING_NAME_PATTERN.matcher(name).matches()) {
        errors.add(format("Invalid port mapping endpoint name: %s", name));
      }
    }

    // Verify service registrations
    for (final ServiceEndpoint registration : job.getRegistration().keySet()) {
      final ServicePorts servicePorts = job.getRegistration().get(registration);
      for (final String portName : servicePorts.getPorts().keySet()) {
        if (!job.getPorts().containsKey(portName)) {
          errors.add(format("Service registration refers to missing port mapping: %s=%s",
                            registration, portName));
        }
        if (!REGISTRATION_NAME_PATTERN.matcher(registration.getName()).matches()) {
          errors.add(format("Invalid service registration name: %s", registration.getName()));
        }
      }
    }

    // Validate volumes
    for (Map.Entry<String, String> entry : job.getVolumes().entrySet()) {
      final String path = entry.getKey();
      final String source = entry.getValue();
      if (!path.startsWith("/")) {
        errors.add("Volume path is not absolute: " + path);
        continue;
      }
      if (!isNullOrEmpty(source) && !source.startsWith("/")) {
        errors.add("Volume source is not absolute: " + source);
        continue;
      }
      final String[] parts = path.split(":", 3);
      if (path.isEmpty() || path.equals("/") ||
          parts.length > 2 ||
          (parts.length > 1 && parts[1].isEmpty())) {
        errors.add(format("Invalid volume path: %s", path));
      }
    }

    // Validate Expiry
    final Date expiry = job.getExpires();
    if (expiry != null && expiry.before(new Date())) {
      errors.add("Job expires in the past");
    }

    errors.addAll(validateJobHealthCheck(job));

    return errors;
  }

  /**
   * Validate the Job's image by checking it's not null or empty and has the right format.
   * @param image The image String
   * @return A set of error Strings
   */
  private Set<String> validateJobImage(final String image) {
    final Set<String> errors = Sets.newHashSet();

    if (image == null) {
      errors.add("Image was not specified.");
    } else {
      // Validate image name
      validateImageReference(image, errors);
    }

    return errors;
  }

  /**
   * Validate the Job's JobId by checking name, version, and hash are
   * not null or empty, don't contain invalid characters.
   * @param job The Job to check.
   * @return A set of error Strings
   */
  private Set<String> validateJobId(final Job job) {
    final Set<String> errors = Sets.newHashSet();
    final JobId jobId = job.getId();

    if (jobId == null) {
      errors.add("Job id was not specified.");
      return errors;
    }

    final String jobIdVersion = jobId.getVersion();
    final String jobIdHash = jobId.getHash();
    final JobId recomputedId = job.toBuilder().build().getId();


    errors.addAll(validateJobName(jobId, recomputedId));
    errors.addAll(validateJobVersion(jobIdVersion, recomputedId));
    errors.addAll(validateJobHash(jobIdHash, recomputedId));

    return errors;
  }

  private Set<String> validateJobName(final JobId jobId, final JobId recomputedId) {
    final Set<String> errors = Sets.newHashSet();

    final String jobIdName = jobId.getName();
    if (jobIdName == null || jobIdName.isEmpty()) {
      errors.add("Job name was not specified.");
      return errors;
    }

    // Check that the job name contains only allowed characters
    if (!NAME_VERSION_PATTERN.matcher(jobIdName).matches()) {
      errors.add(format("Job name may only contain [0-9a-zA-Z-_.] in job name [%s].",
        recomputedId.getName()));
    }

    // Check that the job id is correct
    if (!recomputedId.getName().equals(jobIdName)) {
      errors.add(format("Id name mismatch: %s != %s", jobIdName, recomputedId.getName()));
    }

    return errors;
  }

  private Set<String> validateJobVersion(final String jobIdVersion, final JobId recomputedId) {
    final Set<String> errors = Sets.newHashSet();

    if (jobIdVersion == null || jobIdVersion.isEmpty()) {
      errors.add(format("Job version was not specified in job id [%s].", recomputedId));
      return errors;
    }

    if (!NAME_VERSION_PATTERN.matcher(jobIdVersion).matches()) {
      // Check that the job version contains only allowed characters
      errors.add(format("Job version may only contain [0-9a-zA-Z-_.] in job version [%s].",
          recomputedId.getVersion()));
    }

    // Check that the job version is correct
    if (!recomputedId.getVersion().equals(jobIdVersion)) {
      errors.add(format("Id version mismatch: %s != %s", jobIdVersion, recomputedId.getVersion()));
    }

    return errors;
  }

  private Set<String> validateJobHash(final String jobIdHash, final JobId recomputedId) {
    final Set<String> errors = Sets.newHashSet();

    if (jobIdHash == null || jobIdHash.isEmpty()) {
      errors.add(format("Job hash was not specified in job id [%s].", recomputedId));
      return errors;
    }

    if (jobIdHash.indexOf(':') != -1) {
      // TODO (dxia) Are hashes allowed to have chars not in NAME_VERSION_PATTERN?
      errors.add(format("Job hash contains colon in job id [%s].", recomputedId));
    }

    // Check that the job hash is correct
    if (!recomputedId.getHash().equals(jobIdHash)) {
      errors.add(format("Id hash mismatch: %s != %s", jobIdHash, recomputedId.getHash()));
    }

    return errors;
  }

  @SuppressWarnings("ConstantConditions")
  private boolean validateImageReference(final String imageRef, final Collection<String> errors) {
    boolean valid = true;

    final String repo;
    final String tag;

    final int lastColon = imageRef.lastIndexOf(':');
    if (lastColon != -1 && !(tag = imageRef.substring(lastColon + 1)).contains("/")) {
      repo = imageRef.substring(0, lastColon);
      valid &= validateTag(tag, errors);
    } else {
      repo = imageRef;
    }

    final String invalidRepoName = "Invalid repository name (ex: \"registry.domain.tld/myrepos\")";

    if (repo.contains("://")) {
      // It cannot contain a scheme!
      errors.add(invalidRepoName);
      return false;
    }

    final String[] nameParts = repo.split("/", 2);
    if (!nameParts[0].contains(".") &&
        !nameParts[0].contains(":") &&
        !nameParts[0].equals("localhost")) {
      // This is a Docker Index repos (ex: samalba/hipache or ubuntu)
      return validateRepositoryName(repo, errors);
    }

    if (nameParts.length < 2) {
      // There is a dot in repos name (and no registry address)
      // Is it a Registry address without repos name?
      errors.add(invalidRepoName);
      return false;
    }

    final String endpoint = nameParts[0];
    final String reposName = nameParts[1];
    valid &= validateEndpoint(endpoint, errors);
    valid &= validateRepositoryName(reposName, errors);
    return valid;
  }

  private boolean validateTag(final String tag, final Collection<String> errors) {
    boolean valid = true;
    if (tag.isEmpty()) {
      errors.add("Tag cannot be empty");
      valid = false;
    }
    if (tag.contains("/") || tag.contains(":")) {
      errors.add(format("Illegal tag: \"%s\"", tag));
      valid = false;
    }
    return valid;
  }

  private boolean validateEndpoint(final String endpoint, final Collection<String> errors) {
    final String[] parts = endpoint.split(":", 2);
    if (!validateAddress(parts[0], errors)) {
      return false;
    }
    if (parts.length > 1) {
      final int port;
      try {
        port = Integer.valueOf(parts[1]);
      } catch (NumberFormatException e) {
        errors.add(format("Invalid port in endpoint: \"%s\"", endpoint));
        return false;
      }
      if (port < 0 || port > 65535) {
        errors.add(format("Invalid port in endpoint: \"%s\"", endpoint));
        return false;
      }
    }
    return true;
  }

  private boolean validateAddress(final String address, final Collection<String> errors) {
    if (IPV4_PATTERN.matcher(address).matches()) {
      return true;
    } else if (!DOMAIN_PATTERN.matcher(address).matches() || DIGIT_PERIOD.matcher(address).find()) {
      errors.add(format("Invalid domain name: \"%s\"", address));
      return false;
    }
    return true;
  }

  private boolean validateRepositoryName(final String repositoryName,
                                         final Collection<String> errors) {
    boolean valid = true;
    String repo;
    String name;
    final String[] nameParts = repositoryName.split("/", 2);
    if (nameParts.length < 2) {
      repo = "library";
      name = nameParts[0];
    } else {
      repo = nameParts[0];
      name = nameParts[1];
    }
    if (!NAMESPACE_PATTERN.matcher(repo).matches()) {
      errors.add(
          format("Invalid namespace name (%s), only [a-z0-9_] are allowed, size between 4 and 30",
                 repo));
      valid = false;
    }
    if (!REPO_PATTERN.matcher(name).matches()) {
      errors.add(format("Invalid repository name (%s), only [a-z0-9-_.] are allowed", name));
      valid = false;
    }
    return valid;
  }

  /**
   * Validate the Job's health check.
   * @param job The Job to check.
   * @return A set of error Strings
   */
  private Set<String> validateJobHealthCheck(final Job job) {
    final HealthCheck healthCheck = job.getHealthCheck();

    if (healthCheck == null) {
      return Collections.emptySet();
    }

    final Set<String> errors = Sets.newHashSet();

    if (healthCheck instanceof ExecHealthCheck) {
      final List<String> command = ((ExecHealthCheck) healthCheck).getCommand();
      if (command == null || command.isEmpty()) {
        errors.add("A command must be defined for `docker exec`-based health checks.");
      }
    } else if (healthCheck instanceof HttpHealthCheck || healthCheck instanceof TcpHealthCheck) {
      final String port;
      if (healthCheck instanceof HttpHealthCheck) {
        port = ((HttpHealthCheck) healthCheck).getPort();
      } else {
        port = ((TcpHealthCheck) healthCheck).getPort();
      }

      final Map<String, PortMapping> ports = job.getPorts();
      if (isNullOrEmpty(port)) {
        errors.add("A port must be defined for HTTP and TCP health checks.");
      } else if (!ports.containsKey(port)) {
        errors.add(format("Health check port '%s' not defined in the job. Known ports are '%s'",
                          port, Joiner.on(", ").join(ports.keySet())));
      }
    }

    return errors;
  }

  private boolean legalPort(final int port) {
    return port >= 0 && port <= 65535;
  }
}

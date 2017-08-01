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

package com.spotify.helios.common;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Collections.emptySet;
import static java.util.regex.Pattern.compile;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
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

public class JobValidator {

  private static final Pattern NAME_VERSION_PATTERN = Pattern.compile("[0-9a-zA-Z-_.]+");

  private static final Pattern HOSTNAME_PATTERN =
      Pattern.compile("^([a-z0-9][a-z0-9-]{0,62}$)");

  private static final Pattern DOMAIN_PATTERN =
      Pattern.compile("^(?:(?:[a-zA-Z0-9]|(?:[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9]))"
                      + "(\\.(?:[a-zA-Z0-9]|(?:[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])))*)\\.?$");

  private static final Pattern IPV4_PATTERN =
      Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");

  private static final Pattern NAME_COMPONENT_PATTERN = Pattern.compile("^([a-z0-9._-]+)$");
  private static final int REPO_NAME_MAX_LENGTH = 255;

  // taken from https://github.com/docker/distribution/blob/3150937b9f2b1b5b096b2634d0e7c44d4a0f89fb/reference/regexp.go#L36-L37
  private static final Pattern TAG_PATTERN = Pattern.compile("[\\w][\\w.-]{0,127}");

  private static final Pattern DIGIT_PERIOD = Pattern.compile("^[0-9.]+$");

  private static final Pattern PORT_MAPPING_PROTO_PATTERN = compile("(tcp|udp)");
  private static final Pattern PORT_MAPPING_NAME_PATTERN = compile("\\S+");
  private static final Pattern REGISTRATION_NAME_PATTERN = compile("[_\\-\\w]+");

  private static final List<String> VALID_NETWORK_MODES = ImmutableList.of("bridge", "host");

  private final boolean shouldValidateJobHash;
  private final boolean shouldValidateAddCapabilities;
  private final Set<String> whitelistedCapabilities;

  public JobValidator() {
    this(true);
  }

  public JobValidator(final boolean shouldValidateJobHash) {
    this(shouldValidateJobHash, false);
  }

  public JobValidator(final boolean shouldValidateJobHash,
                      final boolean shouldValidateAddCapabilities) {
    this(shouldValidateJobHash, shouldValidateAddCapabilities, Collections.<String>emptySet());
  }

  public JobValidator(final boolean shouldValidateJobHash,
                      final boolean shouldValidateAddCapabilities,
                      final Set<String> whitelistedCapabilities) {
    this.shouldValidateJobHash = shouldValidateJobHash;
    this.shouldValidateAddCapabilities = shouldValidateAddCapabilities;
    this.whitelistedCapabilities = whitelistedCapabilities;
  }

  public Set<String> validate(final Job job) {
    final Set<String> errors = Sets.newHashSet();

    errors.addAll(validateJobId(job));
    errors.addAll(validateJobImage(job.getImage()));
    errors.addAll(validateJobHostName(job.getHostname()));

    // Check that there's not external port collision
    final Set<Integer> externalPorts = Sets.newHashSet();
    for (final PortMapping mapping : job.getPorts().values()) {
      final Integer externalMappedPort = mapping.getExternalPort();
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
      if (servicePorts == null || servicePorts.getPorts() == null) {
        errors.add(format("registration for '%s' is malformed: does not have a port mapping",
            registration.getName()));
        continue;
      }
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
    for (final Map.Entry<String, String> entry : job.getVolumes().entrySet()) {
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
      if (path.isEmpty()
          || path.equals("/") | parts.length > 2
          || (parts.length > 1 && parts[1].isEmpty())) {
        errors.add(format("Invalid volume path: %s", path));
      }
    }

    // Validate Expiry
    final Date expiry = job.getExpires();
    if (expiry != null && expiry.before(new Date())) {
      errors.add("Job expires in the past");
    }

    errors.addAll(validateJobHealthCheck(job));
    errors.addAll(validateJobNetworkMode(job));
    if (shouldValidateAddCapabilities) {
      errors.addAll(validateAddCapabilities(job));
    }

    // Validate ramdisks
    for (final String mountPoint : job.getRamdisks().keySet()) {
      if (!mountPoint.startsWith("/")) {
        errors.add("Ramdisk mount point is not absolute: " + mountPoint);
        continue;
      }
    }

    // Check that mount-points aren't reused between ramdisks and volumes.
    // This will not caught all cases where docker will fail to create the container because of
    // conflict. For example, a job with a ramdisk mounted at "/a/b" and a volume that binds "/a"
    // in the container to "/tmp" on the host will fail.
    final Set<String> volumeMountPoints = Sets.newHashSet();
    for (String s : job.getVolumes().keySet()) {
      volumeMountPoints.add(s.split(":", 2)[0]);
    }

    for (final String mountPoint : job.getRamdisks().keySet()) {
      if (volumeMountPoints.contains(mountPoint)) {
        errors.add(format("Ramdisk mount point used by volume: %s", mountPoint));
      }
    }

    return errors;
  }

  /**
   * Validate the Job's image by checking it's not null or empty and has the right format.
   *
   * @param image The image String
   *
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
   *
   * @param job The Job to check.
   *
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

    if (this.shouldValidateJobHash) {
      errors.addAll(validateJobHash(jobIdHash, recomputedId));
    }

    return errors;
  }

  private Set<String> validateJobName(final JobId jobId, final JobId recomputedId) {
    final Set<String> errors = Sets.newHashSet();

    final String jobIdName = jobId.getName();
    if (isNullOrEmpty(jobIdName)) {
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

  private Set<String> validateJobHostName(final String hostname) {
    final Set<String> errors = Sets.newHashSet();

    // we're fine if no hostname is set
    if (isNullOrEmpty(hostname)) {
      return errors;
    }

    // Check that the job name contains only allowed characters
    if (!HOSTNAME_PATTERN.matcher(hostname).matches()) {
      errors.add(
          format("Invalid hostname (%s), only [a-z0-9][a-z0-9-] are allowed, size between 1 and 63",
              hostname));
    }

    return errors;
  }

  private Set<String> validateJobVersion(final String jobIdVersion, final JobId recomputedId) {
    final Set<String> errors = Sets.newHashSet();

    if (isNullOrEmpty(jobIdVersion)) {
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

    if (isNullOrEmpty(jobIdHash)) {
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
    final String digest;

    final int lastAtSign = imageRef.lastIndexOf('@');
    final int lastColon = imageRef.lastIndexOf(':');

    if (lastAtSign != -1) {
      repo = imageRef.substring(0, lastAtSign);
      digest = imageRef.substring(lastAtSign + 1);
      valid &= validateDigest(digest, errors);
    } else if (lastColon != -1 && !(tag = imageRef.substring(lastColon + 1)).contains("/")) {
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
    if (!nameParts[0].contains(".")
        && !nameParts[0].contains(":")
        && !nameParts[0].equals("localhost")) {
      // This is a Docker Index repos (ex: samalba/hipache or ubuntu)
      return validateRepositoryName(imageRef, repo, errors);
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
    valid &= validateRepositoryName(imageRef, reposName, errors);
    return valid;
  }

  private boolean validateTag(final String tag, final Collection<String> errors) {
    if (tag.isEmpty()) {
      errors.add("Tag cannot be empty");
      return false;
    }
    if (!TAG_PATTERN.matcher(tag).matches()) {
      errors.add(format("Illegal tag: \"%s\", must match %s", tag, TAG_PATTERN));
      return false;
    }
    return true;
  }

  private boolean validateDigest(final String digest, final Collection<String> errors) {
    if (digest.isEmpty()) {
      errors.add("Digest cannot be empty");
      return false;
    }

    final int firstColon = digest.indexOf(':');
    final int lastColon = digest.lastIndexOf(':');

    if ((firstColon <= 0) || (firstColon != lastColon) || (firstColon == digest.length() - 1)) {
      errors.add(format("Illegal digest: \"%s\"", digest));
      return false;
    }

    return true;
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

  private boolean validateRepositoryName(
      final String imageName,
      final String repositoryName,
      final Collection<String> errors) {

    /*
    From https://github.com/docker/docker/commit/ea98cf74aad3c2633268d5a0b8a2f80b331ddc0b:
    The image name which is made up of slash-separated name components, ....
    Name components may contain lowercase characters, digits and separators. A separator is defined
    as a period, one or two underscores, or one or more dashes. A name component may not start or
    end with a separator.
     */
    final String[] nameParts = repositoryName.split("/");
    for (String name : nameParts) {
      if (!NAME_COMPONENT_PATTERN.matcher(name).matches()) {
        errors.add(
            format("Invalid image name (%s), only %s is allowed for each slash-separated "
                   + "name component (failed on \"%s\")",
                imageName, NAME_COMPONENT_PATTERN, name)
        );
        return false;
      }
    }

    if (repositoryName.length() > REPO_NAME_MAX_LENGTH) {
      errors.add(
          format("Invalid image name (%s), repository name cannot be larger than %d characters",
              imageName, REPO_NAME_MAX_LENGTH)
      );
      return false;
    }
    return true;
  }

  /**
   * Validate the Job's health check.
   *
   * @param job The Job to check.
   *
   * @return A set of error Strings
   */
  private Set<String> validateJobHealthCheck(final Job job) {
    final HealthCheck healthCheck = job.getHealthCheck();

    if (healthCheck == null) {
      return emptySet();
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

  /**
   * Validate the Job's network mode.
   *
   * @param job The Job to check.
   *
   * @return A set of error Strings
   */
  private Set<String> validateJobNetworkMode(final Job job) {
    final String networkMode = job.getNetworkMode();

    if (networkMode == null) {
      return emptySet();
    }

    final Set<String> errors = Sets.newHashSet();

    if (!VALID_NETWORK_MODES.contains(networkMode) && !networkMode.startsWith("container:")) {
      errors.add(String.format(
          "A Docker container's network mode must be %s, or container:<name|id>.",
          Joiner.on(", ").join(VALID_NETWORK_MODES)));
    }

    return errors;
  }

  /**
   * Validate the Job's added Linux capabilities.
   *
   * @param job The Job to check.
   *
   * @return A set of error Strings
   */
  private Set<String> validateAddCapabilities(final Job job) {
    final Set<String> caps = job.getAddCapabilities();

    if (caps == null) {
      return emptySet();
    }

    final Set<String> errors = Sets.newHashSet();

    final Set<String> disallowedCaps = Sets.difference(caps, whitelistedCapabilities);

    if (!disallowedCaps.isEmpty()) {
      errors.add(String.format(
          "The following Linux capabilities aren't allowed by the Helios master: '%s'. "
          + "The allowed capabilities are: '%s'.",
          Joiner.on(", ").join(disallowedCaps), Joiner.on(", ").join(whitelistedCapabilities)));
    }

    return errors;
  }

  private boolean legalPort(final int port) {
    return port >= 0 && port <= 65535;
  }
}

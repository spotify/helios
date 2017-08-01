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

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Throwables.propagate;
import static com.spotify.helios.common.Hash.sha1digest;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.spotify.helios.common.Json;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a Helios job.
 *
 * <p>An sample expression of it in JSON might be:
 * <pre>
 * {
 *   "addCapabilities" : [ "IPC_LOCK", "SYSLOG" ],
 *   "dropCapabilities" : [ "SYS_BOOT", "KILL" ],
 *   "created" : 1410308461448,
 *   "command" : [ "server", "serverconfig.yaml" ],
 *   "env" : {
 *     "JVM_ARGS" : "-Ddw.feature.randomFeatureFlagEnabled=true"
 *   },
 *   "expires" : null,
 *   "gracePeriod": 60,
 *   "healthCheck" : {
 *     "type" : "http",
 *     "path" : "/healthcheck",
 *     "port" : "http-admin"
 *   },
 *   "id" : "myservice:0.5:3539b7bc2235d53f79e6e8511942bbeaa8816265",
 *   "image" : "myregistry:80/janedoe/myservice:0.5-98c6ff4",
 *   "labels" : {
 *     "baz": "qux"
 *   },
 *   "hostname": "myhost",
 *   "metadata": {
 *     "foo": "bar"
 *   },
 *   "networkMode" : "bridge",
 *   "ports" : {
 *     "http" : {
 *       "externalPort" : 8060,
 *       "internalPort" : 8080,
 *       "protocol" : "tcp"
 *     },
 *     "http-admin" : {
 *       "externalPort" : 8061,
 *       "internalPort" : 8081,
 *       "protocol" : "tcp"
 *     }
 *   },
 *   "registration" : {
 *     "service/http" : {
 *       "ports" : {
 *         "http" : { }
 *       }
 *     }
 *   },
 *   "registrationDomain" : "",
 *   "resources" : {
 *     "memory" : 10485760,
 *     "memorySwap" : 10485760,
 *     "cpuset" : "0",
 *     "cpuShares" : 512
 *   }
 *   "securityOpt" : [ "label:user:USER", "apparmor:PROFILE" ],
 *   "token": "insecure-access-token",
 *   "volumes" : {
 *     "/destination/path/in/container.yaml:ro" : "/source/path/in/host.yaml"
 *   }
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Job extends Descriptor implements Comparable<Job> {

  public static final Map<String, String> EMPTY_ENV = emptyMap();
  public static final Resources EMPTY_RESOURCES = null;
  public static final Map<String, PortMapping> EMPTY_PORTS = emptyMap();
  public static final Long EMPTY_CREATED = null;
  public static final List<String> EMPTY_COMMAND = emptyList();
  public static final Map<ServiceEndpoint, ServicePorts> EMPTY_REGISTRATION = emptyMap();
  public static final Integer EMPTY_GRACE_PERIOD = null;
  public static final Map<String, String> EMPTY_VOLUMES = emptyMap();
  public static final String EMPTY_MOUNT = "";
  public static final Date EMPTY_EXPIRES = null;
  public static final String EMPTY_REGISTRATION_DOMAIN = "";
  public static final String EMPTY_CREATING_USER = null;
  public static final String EMPTY_TOKEN = "";
  public static final HealthCheck EMPTY_HEALTH_CHECK = null;
  public static final List<String> EMPTY_SECURITY_OPT = emptyList();
  public static final String DEFAULT_NETWORK_MODE = "bridge";
  public static final String EMPTY_HOSTNAME = null;
  public static final Map<String, String> EMPTY_METADATA = emptyMap();
  public static final Set<String> EMPTY_CAPS = emptySet();
  public static final Map<String, String> EMPTY_LABELS = emptyMap();
  public static final Integer EMPTY_SECONDS_TO_WAIT = null;
  public static final Map<String, String> EMPTY_RAMDISKS = emptyMap();

  private final JobId id;
  private final String image;
  private final String hostname;
  private final Long created;
  private final List<String> command;
  private final Map<String, String> env;
  private final Resources resources;
  private final Map<String, PortMapping> ports;
  private final Map<ServiceEndpoint, ServicePorts> registration;
  private final Integer gracePeriod;
  private final Map<String, String> volumes;
  private final Date expires;
  private final String registrationDomain;
  private final String creatingUser;
  private final String token;
  private final HealthCheck healthCheck;
  private final List<String> securityOpt;
  private final String networkMode;
  private final Map<String, String> metadata;
  private final Set<String> addCapabilities;
  private final Set<String> dropCapabilities;
  private final Map<String, String> labels;
  private final Integer secondsToWaitBeforeKill;
  private final Map<String, String> ramdisks;

  /**
   * Create a Job.
   *
   * @param id                      The id of the job.
   * @param image                   The docker image to use.
   * @param hostname                The hostname to pass to the container.
   * @param created                 The timestamp in milliseconds of when the job was created.
   *                                This should only be set by the server.
   * @param command                 The command to pass to the container.
   * @param env                     Environment variables to set
   * @param resources               Resource specification for the container.
   * @param ports                   The ports you wish to expose from the container.
   * @param registration            Configuration information for the discovery service, if
   *                                applicable.
   * @param gracePeriod             How long to let the container run after deregistering with the
   *                                discovery service.  If nothing is configured in registration,
   *                                this option is ignored.
   * @param volumes                 Docker volumes to mount.
   * @param expires                 If set, a timestamp at which the job and any deployments will be
   *                                removed.
   * @param registrationDomain      If set, overrides the default domain in which discovery service
   *                                registration occurs.  What is allowed here will vary based
   *                                upon the discovery service plugin used.
   * @param creatingUser            The user creating the job.
   * @param token                   The token needed to manipulate this job.
   * @param healthCheck             A health check Helios will execute on the container.
   * @param securityOpt             A list of strings denoting security options for running Docker
   *                                containers, i.e. `docker run --security-opt`.
   * @param networkMode             Sets the networking mode for the container. Supported values
   *                                are: bridge, host, and container:&lt;name|id&gt;.
   * @param metadata                Arbitrary, optional key-value pairs to be stored with the Job.
   * @param addCapabilities         Linux capabilities to add for the container. Optional.
   * @param dropCapabilities        Linux capabilities to drop for the container. Optional.
   * @param labels                  Labels to set on the container. Optional.
   * @param secondsToWaitBeforeKill The time to ask Docker to wait after sending a SIGTERM to the
   *                                container's main process before sending it a SIGKILL. Optional.
   *
   * @see <a href="https://docs.docker.com/engine/reference/run">Docker run reference</a>
   */
  public Job(
      @JsonProperty("id") final JobId id,
      @JsonProperty("image") final String image,
      @JsonProperty("hostname") final String hostname,
      @JsonProperty("created") @Nullable final Long created,
      @JsonProperty("command") @Nullable final List<String> command,
      @JsonProperty("env") @Nullable final Map<String, String> env,
      @JsonProperty("resources") @Nullable final Resources resources,
      @JsonProperty("ports") @Nullable final Map<String, PortMapping> ports,
      @JsonProperty("registration") @Nullable final Map<ServiceEndpoint, ServicePorts> registration,
      @JsonProperty("gracePeriod") @Nullable final Integer gracePeriod,
      @JsonProperty("volumes") @Nullable final Map<String, String> volumes,
      @JsonProperty("expires") @Nullable final Date expires,
      @JsonProperty("registrationDomain") @Nullable String registrationDomain,
      @JsonProperty("creatingUser") @Nullable String creatingUser,
      @JsonProperty("token") @Nullable String token,
      @JsonProperty("healthCheck") @Nullable HealthCheck healthCheck,
      @JsonProperty("securityOpt") @Nullable final List<String> securityOpt,
      @JsonProperty("networkMode") @Nullable final String networkMode,
      @JsonProperty("metadata") @Nullable final Map<String, String> metadata,
      @JsonProperty("addCapabilities") @Nullable final Set<String> addCapabilities,
      @JsonProperty("dropCapabilities") @Nullable final Set<String> dropCapabilities,
      @JsonProperty("labels") @Nullable final Map<String, String> labels,
      @JsonProperty("secondsToWaitBeforeKill") @Nullable final Integer secondsToWaitBeforeKill,
      @JsonProperty("ramdisks") @Nullable final Map<String, String> ramdisks) {
    this.id = id;
    this.image = image;

    // Optional
    this.hostname = Optional.fromNullable(hostname).orNull();
    this.created = Optional.fromNullable(created).orNull();
    this.command = Optional.fromNullable(command).or(EMPTY_COMMAND);
    this.env = Optional.fromNullable(env).or(EMPTY_ENV);
    this.resources = Optional.fromNullable(resources).orNull();
    this.ports = Optional.fromNullable(ports).or(EMPTY_PORTS);
    this.registration = Optional.fromNullable(registration).or(EMPTY_REGISTRATION);
    this.gracePeriod = Optional.fromNullable(gracePeriod).orNull();
    this.volumes = Optional.fromNullable(volumes).or(EMPTY_VOLUMES);
    this.expires = expires;
    this.registrationDomain = Optional.fromNullable(registrationDomain)
        .or(EMPTY_REGISTRATION_DOMAIN);
    this.creatingUser = Optional.fromNullable(creatingUser).orNull();
    this.token = Optional.fromNullable(token).or(EMPTY_TOKEN);
    this.healthCheck = Optional.fromNullable(healthCheck).orNull();
    this.securityOpt = Optional.fromNullable(securityOpt).or(EMPTY_SECURITY_OPT);
    this.networkMode = Optional.fromNullable(networkMode).orNull();
    this.metadata = Optional.fromNullable(metadata).or(EMPTY_METADATA);
    this.addCapabilities = firstNonNull(addCapabilities, EMPTY_CAPS);
    this.dropCapabilities = firstNonNull(dropCapabilities, EMPTY_CAPS);
    this.labels = Optional.fromNullable(labels).or(EMPTY_LABELS);
    this.secondsToWaitBeforeKill = secondsToWaitBeforeKill;
    this.ramdisks = firstNonNull(ramdisks, EMPTY_RAMDISKS);
  }

  private Job(final JobId id, final Builder.Parameters pm) {
    this.id = id;
    this.image = pm.image;

    this.hostname = pm.hostname;
    this.created = pm.created;
    this.command = ImmutableList.copyOf(checkNotNull(pm.command, "command"));
    this.env = ImmutableMap.copyOf(checkNotNull(pm.env, "env"));
    this.resources = pm.resources;
    this.ports = ImmutableMap.copyOf(checkNotNull(pm.ports, "ports"));
    this.registration = ImmutableMap.copyOf(checkNotNull(pm.registration, "registration"));
    this.gracePeriod = pm.gracePeriod;
    this.volumes = ImmutableMap.copyOf(checkNotNull(pm.volumes, "volumes"));
    this.expires = pm.expires;
    this.registrationDomain = Optional.fromNullable(pm.registrationDomain)
        .or(EMPTY_REGISTRATION_DOMAIN);
    this.creatingUser = pm.creatingUser;
    this.token = pm.token;
    this.healthCheck = pm.healthCheck;
    this.securityOpt = pm.securityOpt;
    this.networkMode = pm.networkMode;
    this.metadata = ImmutableMap.copyOf(pm.metadata);
    this.addCapabilities = ImmutableSet.copyOf(pm.addCapabilities);
    this.dropCapabilities = ImmutableSet.copyOf(pm.dropCapabilities);
    this.secondsToWaitBeforeKill = pm.secondsToWaitBeforeKill;
    this.labels = ImmutableMap.copyOf(pm.labels);
    this.ramdisks = ImmutableMap.copyOf(pm.ramdisks);
  }

  public JobId getId() {
    return id;
  }

  public String getImage() {
    return image;
  }

  public String getHostname() {
    return hostname;
  }

  public Long getCreated() {
    return created;
  }

  public List<String> getCommand() {
    return command;
  }

  public Map<String, String> getEnv() {
    return env;
  }

  public Resources getResources() {
    return resources;
  }

  public Map<String, PortMapping> getPorts() {
    return ports;
  }

  public Map<ServiceEndpoint, ServicePorts> getRegistration() {
    return registration;
  }

  public String getRegistrationDomain() {
    return registrationDomain;
  }

  public Integer getGracePeriod() {
    return gracePeriod;
  }

  public Map<String, String> getVolumes() {
    return volumes;
  }

  public Date getExpires() {
    return expires;
  }

  public String getCreatingUser() {
    return creatingUser;
  }

  public String getToken() {
    return token;
  }

  public HealthCheck getHealthCheck() {
    return healthCheck;
  }

  public List<String> getSecurityOpt() {
    return securityOpt;
  }

  public String getNetworkMode() {
    return networkMode;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public Set<String> getAddCapabilities() {
    return addCapabilities;
  }

  public Set<String> getDropCapabilities() {
    return dropCapabilities;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public Integer getSecondsToWaitBeforeKill() {
    return secondsToWaitBeforeKill;
  }

  public Map<String, String> getRamdisks() {
    return ramdisks;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public int compareTo(@NotNull final Job job) {
    return id.compareTo(job.getId());
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final Job that = (Job) obj;

    return Objects.equals(this.id, that.id)
           && Objects.equals(this.image, that.image)
           && Objects.equals(this.hostname, that.hostname)
           && Objects.equals(this.expires, that.expires)
           && Objects.equals(this.created, that.created)
           && Objects.equals(this.command, that.command)
           && Objects.equals(this.env, that.env)
           && Objects.equals(this.resources, that.resources)
           && Objects.equals(this.ports, that.ports)
           && Objects.equals(this.registration, that.registration)
           && Objects.equals(this.registrationDomain, that.registrationDomain)
           && Objects.equals(this.gracePeriod, that.gracePeriod)
           && Objects.equals(this.volumes, that.volumes)
           && Objects.equals(this.creatingUser, that.creatingUser)
           && Objects.equals(this.token, that.token)
           && Objects.equals(this.healthCheck, that.healthCheck)
           && Objects.equals(this.securityOpt, that.securityOpt)
           && Objects.equals(this.networkMode, that.networkMode)
           && Objects.equals(this.metadata, that.metadata)
           && Objects.equals(this.addCapabilities, that.addCapabilities)
           && Objects.equals(this.dropCapabilities, that.dropCapabilities)
           && Objects.equals(this.labels, that.labels)
           && Objects.equals(this.secondsToWaitBeforeKill, that.secondsToWaitBeforeKill)
           && Objects.equals(this.ramdisks, that.ramdisks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, image, hostname, expires, created, command, env, resources,
        ports, registration, registrationDomain, gracePeriod, volumes, creatingUser,
        token, healthCheck, securityOpt, networkMode, metadata, addCapabilities,
        dropCapabilities, labels, secondsToWaitBeforeKill, ramdisks);
  }

  @Override
  public String toString() {
    return "Job{"
           + "id=" + id
           + ", image='" + image + '\''
           + ", hostname='" + hostname + '\''
           + ", created=" + created
           + ", command=" + command
           + ", env=" + env
           + ", resources=" + resources
           + ", ports=" + ports
           + ", registration=" + registration
           + ", gracePeriod=" + gracePeriod
           + ", volumes=" + volumes
           + ", expires=" + expires
           + ", registrationDomain='" + registrationDomain + '\''
           + ", creatingUser='" + creatingUser + '\''
           + ", token='" + token + '\''
           + ", healthCheck=" + healthCheck
           + ", securityOpt=" + securityOpt
           + ", networkMode='" + networkMode + '\''
           + ", metadata=" + metadata
           + ", addCapabilities=" + addCapabilities
           + ", dropCapabilities=" + dropCapabilities
           + ", labels=" + labels
           + ", secondsToWaitBeforeKill=" + secondsToWaitBeforeKill
           + ", ramdisks=" + ramdisks
           + '}';
  }

  public Builder toBuilder() {
    final Builder builder = newBuilder();

    if (id != null) {
      builder.setName(id.getName())
          .setVersion(id.getVersion());
    }

    return builder.setImage(image)
        .setHostname(hostname)
        .setCreated(created)
        .setCommand(command)
        .setEnv(env)
        .setResources(resources)
        .setPorts(ports)
        .setRegistration(registration)
        .setGracePeriod(gracePeriod)
        .setVolumes(volumes)
        .setExpires(expires)
        .setRegistrationDomain(registrationDomain)
        .setCreatingUser(creatingUser)
        .setToken(token)
        .setHealthCheck(healthCheck)
        .setSecurityOpt(securityOpt)
        .setNetworkMode(networkMode)
        .setMetadata(metadata)
        .setAddCapabilities(addCapabilities)
        .setDropCapabilities(dropCapabilities)
        .setLabels(labels)
        .setSecondsToWaitBeforeKill(secondsToWaitBeforeKill)
        .setRamdisks(ramdisks);
  }

  public static class Builder implements Cloneable {

    private final Parameters pm;
    private String hash;

    private Builder() {
      this.pm = new Parameters();
    }

    public Builder(final String hash, final Parameters parameters) {
      this.hash = hash;
      this.pm = parameters;
    }

    private static class Parameters implements Cloneable {

      public String registrationDomain;
      public String name;
      public String version;
      public String image;
      public String hostname;
      public Long created;
      public List<String> command;
      public Map<String, String> env;
      public Resources resources;
      public Map<String, PortMapping> ports;
      public Map<ServiceEndpoint, ServicePorts> registration;
      public Integer gracePeriod;
      public Map<String, String> volumes;
      public Date expires;
      public String creatingUser;
      public String token;
      public HealthCheck healthCheck;
      public List<String> securityOpt;
      public String networkMode;
      public Map<String, String> metadata;
      public Set<String> addCapabilities;
      public Set<String> dropCapabilities;
      public Map<String, String> labels;
      public Integer secondsToWaitBeforeKill;
      public Map<String, String> ramdisks;

      private Parameters() {
        this.created = EMPTY_CREATED;
        this.command = EMPTY_COMMAND;
        this.env = Maps.newHashMap(EMPTY_ENV);
        this.resources = EMPTY_RESOURCES;
        this.ports = Maps.newHashMap(EMPTY_PORTS);
        this.registration = Maps.newHashMap(EMPTY_REGISTRATION);
        this.gracePeriod = EMPTY_GRACE_PERIOD;
        this.volumes = Maps.newHashMap(EMPTY_VOLUMES);
        this.registrationDomain = EMPTY_REGISTRATION_DOMAIN;
        this.creatingUser = EMPTY_CREATING_USER;
        this.token = EMPTY_TOKEN;
        this.healthCheck = EMPTY_HEALTH_CHECK;
        this.securityOpt = EMPTY_SECURITY_OPT;
        this.metadata = Maps.newHashMap();
        this.addCapabilities = EMPTY_CAPS;
        this.dropCapabilities = EMPTY_CAPS;
        this.labels = EMPTY_LABELS;
        this.ramdisks = Maps.newHashMap(EMPTY_RAMDISKS);
      }

      private Parameters(final Parameters pm) {
        this.name = pm.name;
        this.version = pm.version;
        this.image = pm.image;
        this.hostname = pm.hostname;
        this.created = pm.created;
        this.command = ImmutableList.copyOf(pm.command);
        this.env = Maps.newHashMap(pm.env);
        this.resources = pm.resources;
        this.ports = Maps.newHashMap(pm.ports);
        this.registration = Maps.newHashMap(pm.registration);
        this.gracePeriod = pm.gracePeriod;
        this.volumes = Maps.newHashMap(pm.volumes);
        this.expires = pm.expires;
        this.registrationDomain = pm.registrationDomain;
        this.creatingUser = pm.creatingUser;
        this.token = pm.token;
        this.healthCheck = pm.healthCheck;
        this.securityOpt = pm.securityOpt;
        this.networkMode = pm.networkMode;
        this.metadata = pm.metadata;
        this.addCapabilities = pm.addCapabilities;
        this.dropCapabilities = pm.dropCapabilities;
        this.labels = pm.labels;
        this.secondsToWaitBeforeKill = pm.secondsToWaitBeforeKill;
        this.ramdisks = Maps.newHashMap(pm.ramdisks);
      }

      private Parameters withoutMetaParameters() {
        final Parameters clone = new Parameters(this);
        clone.creatingUser = null;
        clone.created = null;

        return clone;
      }
    }

    public Builder setRegistrationDomain(final String domain) {
      this.pm.registrationDomain = domain;
      return this;
    }

    public Builder setCreatingUser(final String creatingUser) {
      this.pm.creatingUser = creatingUser;
      return this;
    }

    public Builder setToken(final String token) {
      this.pm.token = token;
      return this;
    }

    public Builder setHash(final String hash) {
      this.hash = hash;
      return this;
    }

    public Builder setName(final String name) {
      pm.name = name;
      return this;
    }

    public Builder setVersion(final String version) {
      pm.version = version;
      return this;
    }

    public Builder setImage(final String image) {
      pm.image = image;
      return this;
    }

    public Builder setHostname(final String hostname) {
      pm.hostname = hostname;
      return this;
    }

    public Builder setCreated(final Long created) {
      pm.created = created;
      return this;
    }

    public Builder setCommand(final List<String> command) {
      pm.command = ImmutableList.copyOf(command);
      return this;
    }

    public Builder setEnv(final Map<String, String> env) {
      pm.env = Maps.newHashMap(env);
      return this;
    }

    public Builder setResources(final Resources resources) {
      pm.resources = resources;
      return this;
    }

    public Builder addEnv(final String key, final String value) {
      pm.env.put(key, value);
      return this;
    }

    public Builder setPorts(final Map<String, PortMapping> ports) {
      pm.ports = Maps.newHashMap(ports);
      return this;
    }

    public Builder addPort(final String name, final PortMapping port) {
      pm.ports.put(name, port);
      return this;
    }

    public Builder setRegistration(final Map<ServiceEndpoint, ServicePorts> registration) {
      pm.registration = Maps.newHashMap(registration);
      return this;
    }

    public Builder addRegistration(final ServiceEndpoint endpoint, final ServicePorts ports) {
      pm.registration.put(endpoint, ports);
      return this;
    }

    public Builder setGracePeriod(final Integer gracePeriod) {
      pm.gracePeriod = gracePeriod;
      return this;
    }

    public Builder setVolumes(final Map<String, String> volumes) {
      pm.volumes = Maps.newHashMap(volumes);
      return this;
    }

    public Builder addVolume(final String path) {
      pm.volumes.put(path, EMPTY_MOUNT);
      return this;
    }

    public Builder addVolume(final String path, final String source) {
      pm.volumes.put(path, source);
      return this;
    }

    public Builder setExpires(final Date expires) {
      pm.expires = expires;
      return this;
    }

    public Builder setHealthCheck(final HealthCheck healthCheck) {
      pm.healthCheck = healthCheck;
      return this;
    }

    public Builder setSecurityOpt(final List<String> securityOpt) {
      pm.securityOpt = ImmutableList.copyOf(securityOpt);
      return this;
    }

    public Builder setNetworkMode(final String networkMode) {
      pm.networkMode = networkMode;
      return this;
    }

    public Builder setMetadata(final Map<String, String> metadata) {
      pm.metadata = Maps.newHashMap(metadata);
      return this;
    }

    public Builder addMetadata(final String name, final String value) {
      pm.metadata.put(name, value);
      return this;
    }

    public Builder setAddCapabilities(final Collection<String> addCapabilities) {
      pm.addCapabilities = ImmutableSet.copyOf(addCapabilities);
      return this;
    }

    public Builder setDropCapabilities(final Collection<String> dropCapabilities) {
      pm.dropCapabilities = ImmutableSet.copyOf(dropCapabilities);
      return this;
    }

    public Builder setLabels(final Map<String, String> labels) {
      pm.labels = Maps.newHashMap(labels);
      return this;
    }

    public Builder addLabels(final String name, final String value) {
      pm.labels.put(name, value);
      return this;
    }

    public Builder setSecondsToWaitBeforeKill(final Integer secondsToWaitBeforeKill) {
      pm.secondsToWaitBeforeKill = secondsToWaitBeforeKill;
      return this;
    }

    public Builder setRamdisks(final Map<String, String> ramdisks) {
      pm.ramdisks = Maps.newHashMap(ramdisks);
      return this;
    }

    public Builder addRamdisk(final String mountPoint, final String mountOptions) {
      pm.ramdisks.put(mountPoint, mountOptions);
      return this;
    }

    public String getName() {
      return pm.name;
    }

    public String getVersion() {
      return pm.version;
    }

    public String getImage() {
      return pm.image;
    }

    public String getHostname() {
      return pm.hostname;
    }

    public List<String> getCommand() {
      return pm.command;
    }

    public Map<String, String> getEnv() {
      return ImmutableMap.copyOf(pm.env);
    }

    public Map<String, PortMapping> getPorts() {
      return ImmutableMap.copyOf(pm.ports);
    }

    public Map<ServiceEndpoint, ServicePorts> getRegistration() {
      return ImmutableMap.copyOf(pm.registration);
    }

    public String getRegistrationDomain() {
      return pm.registrationDomain;
    }

    public Integer getGracePeriod() {
      return pm.gracePeriod;
    }

    public Map<String, String> getVolumes() {
      return ImmutableMap.copyOf(pm.volumes);
    }

    public Date getExpires() {
      return pm.expires;
    }

    public String getCreatingUser() {
      return pm.creatingUser;
    }

    public Resources getResources() {
      return pm.resources;
    }

    public HealthCheck getHealthCheck() {
      return pm.healthCheck;
    }

    public List<String> getSecurityOpt() {
      return pm.securityOpt;
    }

    public String getNetworkMode() {
      return pm.networkMode;
    }

    public Map<String, String> getMetadata() {
      return ImmutableMap.copyOf(pm.metadata);
    }

    public Set<String> getAddCapabilities() {
      return pm.addCapabilities;
    }

    public Set<String> getDropCapabilities() {
      return pm.dropCapabilities;
    }

    public Map<String, String> getLabels() {
      return ImmutableMap.copyOf(pm.labels);
    }

    public Integer secondsToWaitBeforeKill() {
      return pm.secondsToWaitBeforeKill;
    }

    public Map<String, String> getRamdisks() {
      return ImmutableMap.copyOf(pm.ramdisks);
    }

    @SuppressWarnings({ "CloneDoesntDeclareCloneNotSupportedException",
                        "CloneDoesntCallSuperClone" })
    @Override
    public Builder clone() {
      return new Builder(hash, new Parameters(pm));
    }

    public Job build() {
      final String configHash;
      try {
        configHash = hex(Json.sha1digest(pm.withoutMetaParameters()));
      } catch (IOException e) {
        throw propagate(e);
      }

      final String hash;
      if (!Strings.isNullOrEmpty(this.hash)) {
        hash = this.hash;
      } else {
        if (pm.name != null && pm.version != null) {
          final String input = String.format("%s:%s:%s", pm.name, pm.version, configHash);
          hash = hex(sha1digest(input.getBytes(UTF_8)));
        } else {
          hash = null;
        }
      }

      final JobId id = new JobId(pm.name, pm.version, hash);

      return new Job(id, pm);
    }

    public Job buildWithoutHash() {
      final JobId id = new JobId(pm.name, pm.version);
      return new Job(id, pm);
    }

    private String hex(final byte[] bytes) {
      return BaseEncoding.base16().lowerCase().encode(bytes);
    }
  }
}

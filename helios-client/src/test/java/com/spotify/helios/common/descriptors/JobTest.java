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
import static com.google.common.base.Preconditions.checkArgument;
import static com.spotify.helios.common.descriptors.Descriptor.parse;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.BaseEncoding;
import com.spotify.helios.common.Hash;
import com.spotify.helios.common.Json;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class JobTest {

  private Map<String, Object> map(final Object... objects) {
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    checkArgument(objects.length % 2 == 0);
    for (int i = 0; i < objects.length; i += 2) {
      builder.put((String) objects[i], objects[i + 1]);
    }
    return builder.build();
  }

  @Test
  public void testNormalizedExcludesEmptyStrings() throws Exception {
    final Job j = Job.newBuilder().setName("x").setImage("x").setVersion("x")
        .setRegistrationDomain("").build();
    assertFalse(Json.asNormalizedString(j).contains("registrationDomain"));
  }

  @Test
  public void verifyBuilder() throws Exception {
    final Job.Builder builder = Job.newBuilder();

    // Input to setXXX
    final String setName = "set_name";
    final String setVersion = "set_version";
    final String setImage = "set_image";
    final String setHostname = "set_hostname";
    final List<String> setCommand = asList("set", "command");
    final Map<String, String> setEnv = ImmutableMap.of("set", "env");
    final Map<String, PortMapping> setPorts = ImmutableMap.of("set_ports", PortMapping.of(1234));
    final ImmutableMap.Builder<String, ServicePortParameters> setServicePortsBuilder =
        ImmutableMap.builder();
    setServicePortsBuilder.put("set_ports1", new ServicePortParameters(
        ImmutableList.of("tag1", "tag2")));
    setServicePortsBuilder.put("set_ports2", new ServicePortParameters(
        ImmutableList.of("tag3", "tag4")));
    final ServicePorts setServicePorts = new ServicePorts(setServicePortsBuilder.build());
    final Map<ServiceEndpoint, ServicePorts> setRegistration = ImmutableMap.of(
        ServiceEndpoint.of("set_service", "set_proto"), setServicePorts);
    final Integer setGracePeriod = 120;
    final Map<String, String> setVolumes = ImmutableMap.of("/set", "/volume");
    final Date setExpires = new Date();
    final String setRegistrationDomain = "my.domain";
    final String setCreatingUser = "username";
    final Resources setResources = new Resources(10485760L, 10485761L, 4L, "1");
    final HealthCheck setHealthCheck = HealthCheck.newHttpHealthCheck()
        .setPath("/healthcheck")
        .setPort("set_ports")
        .build();
    final List<String> setSecurityOpt = Lists.newArrayList("label:user:dxia", "apparmor:foo");
    final String setNetworkMode = "host";
    final Map<String, String> setMetadata = ImmutableMap.of("set_metadata_key", "set_metadata_val");
    final Set<String> setAddCapabilities = ImmutableSet.of("set_cap_add1", "set_cap_add2");
    final Set<String> setDropCapabilities = ImmutableSet.of("set_cap_drop1", "set_cap_drop2");
    final Map<String, String> setLabels = ImmutableMap.of("set_label_key", "set_label_val");

    // Input to addXXX
    final Map<String, String> addEnv = ImmutableMap.of("add", "env");
    final Map<String, PortMapping> addPorts = ImmutableMap.of("add_ports", PortMapping.of(4711));
    final ImmutableMap.Builder<String, ServicePortParameters> addServicePortsBuilder =
        ImmutableMap.builder();
    addServicePortsBuilder.put("add_ports1", new ServicePortParameters(
        ImmutableList.of("tag1", "tag2")));
    addServicePortsBuilder.put("add_ports2", new ServicePortParameters(
        ImmutableList.of("tag3", "tag4")));
    final ServicePorts addServicePorts = new ServicePorts(addServicePortsBuilder.build());
    final Map<ServiceEndpoint, ServicePorts> addRegistration = ImmutableMap.of(
        ServiceEndpoint.of("add_service", "add_proto"), addServicePorts);
    final Map<String, String> addVolumes = ImmutableMap.of("/add", "/volume");
    final Map<String, String> addMetadata = ImmutableMap.of("add_metadata_key", "add_metadata_val");
    final Map<String, String> addLabels = ImmutableMap.of("add_labels_key", "add_labels_val");

    // Expected output from getXXX
    final String expectedName = setName;
    final String expectedVersion = setVersion;
    final String expectedImage = setImage;
    final String expectedHostname = setHostname;
    final List<String> expectedCommand = setCommand;
    final Map<String, String> expectedEnv = concat(setEnv, addEnv);
    final Map<String, PortMapping> expectedPorts = concat(setPorts, addPorts);
    final Map<ServiceEndpoint, ServicePorts> expectedRegistration =
        concat(setRegistration, addRegistration);
    final Integer expectedGracePeriod = setGracePeriod;
    final Map<String, String> expectedVolumes = concat(setVolumes, addVolumes);
    final Date expectedExpires = setExpires;
    final String expectedRegistrationDomain = setRegistrationDomain;
    final String expectedCreatingUser = setCreatingUser;
    final Resources expectedResources = setResources;
    final HealthCheck expectedHealthCheck = setHealthCheck;
    final List<String> expectedSecurityOpt = setSecurityOpt;
    final String expectedNetworkMode = setNetworkMode;
    final Map<String, String> expectedMetadata = concat(setMetadata, addMetadata);
    final Set<String> expectedAddCapabilities = setAddCapabilities;
    final Set<String> expectedDropCapabilities = setDropCapabilities;
    final Map<String, String> expectedLabels = concat(setLabels, addLabels);

    // Check setXXX methods
    builder.setName(setName);
    builder.setVersion(setVersion);
    builder.setImage(setImage);
    builder.setHostname(setHostname);
    builder.setCommand(setCommand);
    builder.setEnv(setEnv);
    builder.setPorts(setPorts);
    builder.setRegistration(setRegistration);
    builder.setGracePeriod(setGracePeriod);
    builder.setVolumes(setVolumes);
    builder.setExpires(setExpires);
    builder.setRegistrationDomain(setRegistrationDomain);
    builder.setCreatingUser(setCreatingUser);
    builder.setResources(setResources);
    builder.setHealthCheck(setHealthCheck);
    builder.setSecurityOpt(setSecurityOpt);
    builder.setNetworkMode(setNetworkMode);
    builder.setMetadata(setMetadata);
    builder.setAddCapabilities(setAddCapabilities);
    builder.setDropCapabilities(setDropCapabilities);
    builder.setLabels(setLabels);

    // Check addXXX methods
    for (final Map.Entry<String, String> entry : addEnv.entrySet()) {
      builder.addEnv(entry.getKey(), entry.getValue());
    }
    for (final Map.Entry<String, PortMapping> entry : addPorts.entrySet()) {
      builder.addPort(entry.getKey(), entry.getValue());
    }
    for (final Map.Entry<ServiceEndpoint, ServicePorts> entry : addRegistration.entrySet()) {
      builder.addRegistration(entry.getKey(), entry.getValue());
    }
    for (final Map.Entry<String, String> entry : addVolumes.entrySet()) {
      builder.addVolume(entry.getKey(), entry.getValue());
    }
    for (final Map.Entry<String, String> entry : addMetadata.entrySet()) {
      builder.addMetadata(entry.getKey(), entry.getValue());
    }
    for (final Map.Entry<String, String> entry : addLabels.entrySet()) {
      builder.addLabels(entry.getKey(), entry.getValue());
    }

    assertEquals("name", expectedName, builder.getName());
    assertEquals("version", expectedVersion, builder.getVersion());
    assertEquals("image", expectedImage, builder.getImage());
    assertEquals("hostname", expectedHostname, builder.getHostname());
    assertEquals("command", expectedCommand, builder.getCommand());
    assertEquals("env", expectedEnv, builder.getEnv());
    assertEquals("ports", expectedPorts, builder.getPorts());
    assertEquals("registration", expectedRegistration, builder.getRegistration());
    assertEquals("gracePeriod", expectedGracePeriod, builder.getGracePeriod());
    assertEquals("volumes", expectedVolumes, builder.getVolumes());
    assertEquals("expires", expectedExpires, builder.getExpires());
    assertEquals("registrationDomain", expectedRegistrationDomain, builder.getRegistrationDomain());
    assertEquals("creatingUser", expectedCreatingUser, builder.getCreatingUser());
    assertEquals("resources", expectedResources, builder.getResources());
    assertEquals("healthCheck", expectedHealthCheck, builder.getHealthCheck());
    assertEquals("securityOpt", expectedSecurityOpt, builder.getSecurityOpt());
    assertEquals("networkMode", expectedNetworkMode, builder.getNetworkMode());
    assertEquals("metadata", expectedMetadata, builder.getMetadata());
    assertEquals("addCapabilities", expectedAddCapabilities, builder.getAddCapabilities());
    assertEquals("dropCapabilities", expectedDropCapabilities,
        builder.getDropCapabilities());
    assertEquals("labels", expectedLabels, builder.getLabels());

    // Check final output
    final Job job = builder.build();
    assertEquals("name", expectedName, job.getId().getName());
    assertEquals("version", expectedVersion, job.getId().getVersion());
    assertEquals("image", expectedImage, job.getImage());
    assertEquals("hostname", expectedHostname, job.getHostname());
    assertEquals("command", expectedCommand, job.getCommand());
    assertEquals("env", expectedEnv, job.getEnv());
    assertEquals("ports", expectedPorts, job.getPorts());
    assertEquals("registration", expectedRegistration, job.getRegistration());
    assertEquals("gracePeriod", expectedGracePeriod, job.getGracePeriod());
    assertEquals("volumes", expectedVolumes, job.getVolumes());
    assertEquals("expires", expectedExpires, job.getExpires());
    assertEquals("registrationDomain", expectedRegistrationDomain, job.getRegistrationDomain());
    assertEquals("creatingUser", expectedCreatingUser, job.getCreatingUser());
    assertEquals("resources", expectedResources, job.getResources());
    assertEquals("healthCheck", expectedHealthCheck, job.getHealthCheck());
    assertEquals("securityOpt", expectedSecurityOpt, job.getSecurityOpt());
    assertEquals("networkMode", expectedNetworkMode, job.getNetworkMode());
    assertEquals("metadata", expectedMetadata, job.getMetadata());
    assertEquals("addCapabilities", expectedAddCapabilities, job.getAddCapabilities());
    assertEquals("dropCapabilities", expectedDropCapabilities, job.getDropCapabilities());
    assertEquals("labels", expectedLabels, job.getLabels());

    // Check toBuilder
    final Job.Builder rebuilder = job.toBuilder();
    assertEquals("name", expectedName, rebuilder.getName());
    assertEquals("version", expectedVersion, rebuilder.getVersion());
    assertEquals("image", expectedImage, rebuilder.getImage());
    assertEquals("hostname", expectedHostname, rebuilder.getHostname());
    assertEquals("command", expectedCommand, rebuilder.getCommand());
    assertEquals("env", expectedEnv, rebuilder.getEnv());
    assertEquals("ports", expectedPorts, rebuilder.getPorts());
    assertEquals("registration", expectedRegistration, rebuilder.getRegistration());
    assertEquals("gracePeriod", expectedGracePeriod, rebuilder.getGracePeriod());
    assertEquals("volumes", expectedVolumes, rebuilder.getVolumes());
    assertEquals("expires", expectedExpires, rebuilder.getExpires());
    assertEquals("registrationDomain", expectedRegistrationDomain,
        rebuilder.getRegistrationDomain());
    assertEquals("creatingUser", expectedCreatingUser, rebuilder.getCreatingUser());
    assertEquals("resources", expectedResources, rebuilder.getResources());
    assertEquals("healthCheck", expectedHealthCheck, rebuilder.getHealthCheck());
    assertEquals("securityOpt", expectedSecurityOpt, rebuilder.getSecurityOpt());
    assertEquals("networkMode", expectedNetworkMode, rebuilder.getNetworkMode());
    assertEquals("metadata", expectedMetadata, rebuilder.getMetadata());
    assertEquals("addCapabilities", expectedAddCapabilities, rebuilder.getAddCapabilities());
    assertEquals("dropCapabilities", expectedDropCapabilities,
        rebuilder.getDropCapabilities());
    assertEquals("labels", expectedLabels, rebuilder.getLabels());

    // Check clone
    final Job.Builder cloned = builder.clone();
    assertEquals("name", expectedName, cloned.getName());
    assertEquals("version", expectedVersion, cloned.getVersion());
    assertEquals("image", expectedImage, cloned.getImage());
    assertEquals("hostname", expectedHostname, cloned.getHostname());
    assertEquals("command", expectedCommand, cloned.getCommand());
    assertEquals("env", expectedEnv, cloned.getEnv());
    assertEquals("ports", expectedPorts, cloned.getPorts());
    assertEquals("registration", expectedRegistration, cloned.getRegistration());
    assertEquals("gracePeriod", expectedGracePeriod, cloned.getGracePeriod());
    assertEquals("volumes", expectedVolumes, cloned.getVolumes());
    assertEquals("expires", expectedExpires, cloned.getExpires());
    assertEquals("registrationDomain", expectedRegistrationDomain,
        cloned.getRegistrationDomain());
    assertEquals("creatingUser", expectedCreatingUser, cloned.getCreatingUser());
    assertEquals("resources", expectedResources, cloned.getResources());
    assertEquals("healthCheck", expectedHealthCheck, cloned.getHealthCheck());
    assertEquals("securityOpt", expectedSecurityOpt, cloned.getSecurityOpt());
    assertEquals("networkMode", expectedNetworkMode, cloned.getNetworkMode());
    assertEquals("metadata", expectedMetadata, cloned.getMetadata());
    assertEquals("addCapabilities", expectedAddCapabilities, cloned.getAddCapabilities());
    assertEquals("dropCapabilities", expectedDropCapabilities,
        cloned.getDropCapabilities());
    assertEquals("labels", expectedLabels, cloned.getLabels());

    final Job clonedJob = cloned.build();
    assertEquals("name", expectedName, clonedJob.getId().getName());
    assertEquals("version", expectedVersion, clonedJob.getId().getVersion());
    assertEquals("image", expectedImage, clonedJob.getImage());
    assertEquals("hostname", expectedHostname, clonedJob.getHostname());
    assertEquals("command", expectedCommand, clonedJob.getCommand());
    assertEquals("env", expectedEnv, clonedJob.getEnv());
    assertEquals("ports", expectedPorts, clonedJob.getPorts());
    assertEquals("registration", expectedRegistration, clonedJob.getRegistration());
    assertEquals("gracePeriod", expectedGracePeriod, clonedJob.getGracePeriod());
    assertEquals("volumes", expectedVolumes, clonedJob.getVolumes());
    assertEquals("expires", expectedExpires, clonedJob.getExpires());
    assertEquals("registrationDomain", expectedRegistrationDomain,
        clonedJob.getRegistrationDomain());
    assertEquals("creatingUser", expectedCreatingUser, clonedJob.getCreatingUser());
    assertEquals("resources", expectedResources, clonedJob.getResources());
    assertEquals("healthCheck", expectedHealthCheck, clonedJob.getHealthCheck());
    assertEquals("securityOpt", expectedSecurityOpt, clonedJob.getSecurityOpt());
    assertEquals("networkMode", expectedNetworkMode, clonedJob.getNetworkMode());
    assertEquals("metadata", expectedMetadata, clonedJob.getMetadata());
    assertEquals("addCapabilities", expectedAddCapabilities, clonedJob.getAddCapabilities());
    assertEquals("dropCapabilities", expectedDropCapabilities,
        clonedJob.getDropCapabilities());
    assertEquals("labels", expectedLabels, clonedJob.getLabels());
  }

  @SafeVarargs
  private final <K, V> Map<K, V> concat(final Map<K, V>... maps) {
    final ImmutableMap.Builder<K, V> b = ImmutableMap.builder();
    for (final Map<K, V> map : maps) {
      b.putAll(map);
    }
    return b.build();
  }

  /** Verify the Builder allows calling addFoo() before setFoo() for collection types. */
  @Test
  public void testBuilderAddBeforeSet() throws Exception {
    final Job job = Job.newBuilder()
        .addEnv("env", "var")
        .addMetadata("meta", "data")
        .addPort("http", PortMapping.of(80, 8000))
        .addRegistration(ServiceEndpoint.of("foo", "http"), ServicePorts.of("http"))
        .addVolume("/foo", "/bar")
        .build();

    assertThat(job.getEnv(), hasEntry("env", "var"));
    assertThat(job.getMetadata(), hasEntry("meta", "data"));
    assertThat(job.getPorts(), hasEntry("http", PortMapping.of(80, 8000)));
    assertThat(job.getRegistration(),
        hasEntry(ServiceEndpoint.of("foo", "http"), ServicePorts.of("http")));
    assertThat(job.getVolumes(), hasEntry("/foo", "/bar"));
  }

  @Test
  public void verifySha1Id() throws IOException {
    final Map<String, Object> expectedConfig = map("command", asList("foo", "bar"),
        "image", "foobar:4711",
        "name", "foozbarz",
        "version", "17");

    final String expectedInput = "foozbarz:17:" + hex(Json.sha1digest(expectedConfig));
    final String expectedDigest = hex(Hash.sha1digest(expectedInput.getBytes(UTF_8)));
    final JobId expectedId = JobId.fromString("foozbarz:17:" + expectedDigest);

    final Job job = Job.newBuilder()
        .setCommand(asList("foo", "bar"))
        .setImage("foobar:4711")
        .setName("foozbarz")
        .setVersion("17")
        .build();

    assertEquals(expectedId, job.getId());
  }

  @Test
  public void verifySha1IdWithEnv() throws IOException {
    final Map<String, String> env = ImmutableMap.of("FOO", "BAR");
    final Map<String, Object> expectedConfig = map("command", asList("foo", "bar"),
        "image", "foobar:4711",
        "name", "foozbarz",
        "version", "17",
        "env", env);

    final String expectedInput = "foozbarz:17:" + hex(Json.sha1digest(expectedConfig));
    final String expectedDigest = hex(Hash.sha1digest(expectedInput.getBytes(UTF_8)));
    final JobId expectedId = JobId.fromString("foozbarz:17:" + expectedDigest);

    final Job job = Job.newBuilder()
        .setCommand(asList("foo", "bar"))
        .setImage("foobar:4711")
        .setName("foozbarz")
        .setVersion("17")
        .setEnv(env)
        .build();

    assertEquals(expectedId, job.getId());
  }

  private String hex(final byte[] bytes) {
    return BaseEncoding.base16().lowerCase().encode(bytes);
  }

  @Test
  public void verifyCanParseJobWithUnknownFields() throws Exception {
    final Job job = Job.newBuilder()
        .setCommand(asList("foo", "bar"))
        .setImage("foobar:4711")
        .setName("foozbarz")
        .setVersion("17")
        .build();

    final String jobJson = job.toJsonString();

    final ObjectMapper objectMapper = new ObjectMapper();
    final Map<String, Object> fields = objectMapper.readValue(
        jobJson, new TypeReference<Map<String, Object>>() {});
    fields.put("UNKNOWN_FIELD", "FOOBAR");
    final String modifiedJobJson = objectMapper.writeValueAsString(fields);

    final Job parsedJob = parse(modifiedJobJson, Job.class);

    assertEquals(job, parsedJob);
  }

  @Test
  public void verifyCanParseJobWithMissingEnv() throws Exception {
    final Job job = Job.newBuilder()
        .setCommand(asList("foo", "bar"))
        .setImage("foobar:4711")
        .setName("foozbarz")
        .setVersion("17")
        .build();

    removeFieldAndParse(job, "env");
  }

  @Test
  public void verifyCanParseJobWithMissingMetadata() throws Exception {
    final Job job = Job.newBuilder()
        .setCommand(asList("foo", "bar"))
        .setImage("foobar:4711")
        .setName("foozbarz")
        .setVersion("17")
        .build();

    removeFieldAndParse(job, "metadata");
  }

  private static void removeFieldAndParse(final Job job, final String... fieldNames)
      throws Exception {
    final String jobJson = job.toJsonString();

    final ObjectMapper objectMapper = new ObjectMapper();
    final Map<String, Object> fields = objectMapper.readValue(
        jobJson, new TypeReference<Map<String, Object>>() {});

    for (final String field : fieldNames) {
      fields.remove(field);
    }
    final String modifiedJobJson = objectMapper.writeValueAsString(fields);

    final Job parsedJob = parse(modifiedJobJson, Job.class);

    assertEquals(job, parsedJob);
  }

  @Test
  public void verifyJobIsImmutable() {
    final List<String> expectedCommand = ImmutableList.of("foo");
    final Map<String, String> expectedEnv = ImmutableMap.of("e1", "1");
    final Map<String, String> expectedMetadata = ImmutableMap.of("foo", "bar");
    final Map<String, PortMapping> expectedPorts = ImmutableMap.of("p1", PortMapping.of(1, 2));
    final Map<ServiceEndpoint, ServicePorts> expectedRegistration =
        ImmutableMap.of(ServiceEndpoint.of("foo", "tcp"), ServicePorts.of("p1"));
    final Integer expectedGracePeriod = 240;

    final List<String> mutableCommand = Lists.newArrayList(expectedCommand);
    final Map<String, String> mutableEnv = Maps.newHashMap(expectedEnv);
    final Map<String, String> mutableMetadata = Maps.newHashMap(expectedMetadata);
    final Map<String, PortMapping> mutablePorts = Maps.newHashMap(expectedPorts);
    final Map<ServiceEndpoint, ServicePorts> mutableRegistration =
        Maps.newHashMap(expectedRegistration);

    final Job.Builder builder = Job.newBuilder()
        .setCommand(mutableCommand)
        .setEnv(mutableEnv)
        .setMetadata(mutableMetadata)
        .setPorts(mutablePorts)
        .setImage("foobar:4711")
        .setName("foozbarz")
        .setVersion("17")
        .setRegistration(mutableRegistration)
        .setGracePeriod(expectedGracePeriod);

    final Job job = builder.build();

    mutableCommand.add("bar");
    mutableEnv.put("e2", "2");
    mutableMetadata.put("some", "thing");
    mutablePorts.put("p2", PortMapping.of(3, 4));
    mutableRegistration.put(ServiceEndpoint.of("bar", "udp"), ServicePorts.of("p2"));

    builder.addEnv("added_env", "FOO");
    builder.addMetadata("added", "data");
    builder.addPort("added_port", PortMapping.of(4711));
    builder.addRegistration(ServiceEndpoint.of("added_reg", "added_proto"),
        ServicePorts.of("added_port"));
    builder.setGracePeriod(480);

    assertEquals(expectedCommand, job.getCommand());
    assertEquals(expectedEnv, job.getEnv());
    assertEquals(expectedMetadata, job.getMetadata());
    assertEquals(expectedPorts, job.getPorts());
    assertEquals(expectedRegistration, job.getRegistration());
    assertEquals(expectedGracePeriod, job.getGracePeriod());
  }

  @Test
  public void testChangingPortTagsChangesJobHash() {
    final Job j = Job.newBuilder().setName("foo").setVersion("1").setImage("foobar").build();
    final Job.Builder builder = j.toBuilder();
    final Map<String, PortMapping> ports = ImmutableMap.of("add_ports1", PortMapping.of(1234),
        "add_ports2", PortMapping.of(2345));
    final ImmutableMap.Builder<String, ServicePortParameters> servicePortsBuilder =
        ImmutableMap.builder();
    servicePortsBuilder.put("add_ports1", new ServicePortParameters(
        ImmutableList.of("tag1", "tag2")));
    servicePortsBuilder.put("add_ports2", new ServicePortParameters(
        ImmutableList.of("tag3", "tag4")));
    final ServicePorts servicePorts = new ServicePorts(servicePortsBuilder.build());
    final Map<ServiceEndpoint, ServicePorts> oldRegistration = ImmutableMap.of(
        ServiceEndpoint.of("add_service", "add_proto"), servicePorts);
    final Job job = builder.setPorts(ports).setRegistration(oldRegistration).build();

    final ImmutableMap.Builder<String, ServicePortParameters> newServicePortsBuilder =
        ImmutableMap.builder();
    newServicePortsBuilder.put("add_ports1", new ServicePortParameters(
        ImmutableList.of("tag1", "newtag")));
    newServicePortsBuilder.put("add_ports2", new ServicePortParameters(
        ImmutableList.of("tag3", "tag4")));
    final ServicePorts newServicePorts = new ServicePorts(newServicePortsBuilder.build());
    final Map<ServiceEndpoint, ServicePorts> newRegistration = ImmutableMap.of(
        ServiceEndpoint.of("add_service", "add_proto"), newServicePorts);
    final Job newJob = builder.setRegistration(newRegistration).build();

    assertNotEquals(job.getId().getHash(), newJob.getId().getHash());
  }

  @Test
  public void testBuildWithoutHash() {
    final Job.Builder builder = Job.newBuilder()
        .setCommand(asList("foo", "bar"))
        .setImage("foobar:4711")
        .setName("foozbarz")
        .setVersion("17");

    assertNull(builder.buildWithoutHash().getId().getHash());
    assertNotNull(builder.build().getId().getHash());
  }
}

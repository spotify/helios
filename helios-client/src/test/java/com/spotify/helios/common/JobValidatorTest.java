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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.spotify.helios.common.descriptors.HealthCheck;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePortParameters;
import com.spotify.helios.common.descriptors.ServicePorts;

import org.junit.Test;

import java.util.Map;

import static com.google.common.collect.Sets.newHashSet;
import static com.spotify.helios.common.descriptors.Job.EMPTY_COMMAND;
import static com.spotify.helios.common.descriptors.Job.EMPTY_CREATING_USER;
import static com.spotify.helios.common.descriptors.Job.EMPTY_ENV;
import static com.spotify.helios.common.descriptors.Job.EMPTY_EXPIRES;
import static com.spotify.helios.common.descriptors.Job.EMPTY_GRACE_PERIOD;
import static com.spotify.helios.common.descriptors.Job.EMPTY_HEALTH_CHECK;
import static com.spotify.helios.common.descriptors.Job.EMPTY_NETWORK_MODE;
import static com.spotify.helios.common.descriptors.Job.EMPTY_PORTS;
import static com.spotify.helios.common.descriptors.Job.EMPTY_REGISTRATION;
import static com.spotify.helios.common.descriptors.Job.EMPTY_REGISTRATION_DOMAIN;
import static com.spotify.helios.common.descriptors.Job.EMPTY_RESOURCES;
import static com.spotify.helios.common.descriptors.Job.EMPTY_SECURITY_OPT;
import static com.spotify.helios.common.descriptors.Job.EMPTY_TOKEN;
import static com.spotify.helios.common.descriptors.Job.EMPTY_VOLUMES;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

public class JobValidatorTest {
  private final HealthCheck HEALTH_CHECK =
    HealthCheck.newHttpHealthCheck().setPath("/").setPort("1").build();
  private final Job VALID_JOB = Job.newBuilder()
      .setName("foo")
      .setVersion("1")
      .setImage("bar")
      .setEnv(ImmutableMap.of("FOO", "BAR"))
      .setPorts(ImmutableMap.of("1", PortMapping.of(1, 1),
                                "2", PortMapping.of(2, 2)))
      .setHealthCheck(HEALTH_CHECK)
      .build();

  final JobValidator validator = new JobValidator();

  @Test
  public void testValidJobPasses() {
    assertThat(validator.validate(VALID_JOB), is(empty()));
  }

  @Test
  public void testValidNamesPass() {
    final Job.Builder b = Job.newBuilder().setVersion("1").setImage("bar");
    assertThat(validator.validate(b.setName("foo").build()), is(empty()));
    assertThat(validator.validate(b.setName("17").build()), is(empty()));
    assertThat(validator.validate(b.setName("foo17.bar-baz_quux").build()), is(empty()));
  }

  @Test
  public void testValidVersionsPass() {
    final Job.Builder b = Job.newBuilder().setName("foo").setImage("bar");
    assertThat(validator.validate(b.setVersion("foo").build()), is(empty()));
    assertThat(validator.validate(b.setVersion("17").build()), is(empty()));
    assertThat(validator.validate(b.setVersion("foo17.bar-baz_quux").build()), is(empty()));
  }

  @Test
  public void testValidImagePasses() {
    final Job.Builder b = Job.newBuilder().setName("foo").setVersion("1");
    assertThat(validator.validate(b.setImage("repo").build()), is(empty()));
    assertThat(validator.validate(b.setImage("namespace/repo").build()), is(empty()));
    assertThat(validator.validate(b.setImage("namespace/repo:tag").build()), is(empty()));
    assertThat(validator.validate(b.setImage("namespace/repo:1.2").build()), is(empty()));
    assertThat(validator.validate(b.setImage("reg.istry:4711/repo").build()), is(empty()));
    assertThat(validator.validate(b.setImage("reg.istry.:4711/repo").build()), is(empty()));
    assertThat(validator.validate(b.setImage("reg.istry:4711/namespace/repo").build()),
               is(empty()));
    assertThat(validator.validate(b.setImage("reg.istry.:4711/namespace/repo").build()),
               is(empty()));
    assertThat(validator.validate(b.setImage("1.2.3.4:4711/namespace/repo").build()), is(empty()));
    assertThat(validator.validate(b.setImage("registry.test.net:80/fooo/bar").build()),
               is(empty()));
    assertThat(validator.validate(b.setImage("registry.test.net.:80/fooo/bar").build()),
               is(empty()));
  }

  @Test
  public void testValidVolumesPass() {
    final Job j = Job.newBuilder().setName("foo").setVersion("1").setImage("foobar").build();
    assertThat(validator.validate(j.toBuilder().addVolume("/foo").build()), is(empty()));
    assertThat(validator.validate(j.toBuilder().addVolume("/foo", "/").build()), is(empty()));
    assertThat(validator.validate(j.toBuilder().addVolume("/foo:ro", "/").build()), is(empty()));
    assertThat(validator.validate(j.toBuilder().addVolume("/foo", "/bar").build()), is(empty()));
    assertThat(validator.validate(j.toBuilder().addVolume("/foo:ro", "/bar").build()), is(empty()));
  }

  @Test
  public void testValidPortTagsPass() {
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
    final Map<ServiceEndpoint, ServicePorts> addRegistration = ImmutableMap.of(
        ServiceEndpoint.of("add_service", "add_proto"), servicePorts);
    builder.setPorts(ports).setRegistration(addRegistration);
    assertThat(validator.validate(builder.build()), is(empty()));
  }

  @Test
  public void testPortMappingCollisionFails() throws Exception {
    final Job job = Job.newBuilder()
        .setName("foo")
        .setVersion("1")
        .setImage("bar")
        .setPorts(ImmutableMap.of("1", PortMapping.of(1, 1),
                                  "2", PortMapping.of(2, 1)))
        .build();

    assertEquals(ImmutableSet.of("Duplicate external port mapping: 1"), validator.validate(job));
  }

  @Test
  public void testIdMismatchFails() throws Exception {
    final Job job = new Job(JobId.fromString("foo:bar:badf00d"),
                            "bar", EMPTY_COMMAND, EMPTY_ENV, EMPTY_RESOURCES, EMPTY_PORTS,
                            EMPTY_REGISTRATION, EMPTY_GRACE_PERIOD, EMPTY_VOLUMES, EMPTY_EXPIRES,
                            EMPTY_REGISTRATION_DOMAIN, EMPTY_CREATING_USER, EMPTY_TOKEN,
                            EMPTY_HEALTH_CHECK, EMPTY_SECURITY_OPT, EMPTY_NETWORK_MODE);
    final JobId recomputedId = job.toBuilder().build().getId();
    assertEquals(ImmutableSet.of("Id hash mismatch: " + job.getId().getHash()
        + " != " + recomputedId.getHash()), validator.validate(job));
  }

  @Test
  public void testInvalidNamesFail() throws Exception {
    final Job.Builder b = Job.newBuilder().setVersion("1").setImage("foo");
    assertEquals(newHashSet("Job name was not specified.",
        "Job hash was not specified in job id [null:1]."),
                 validator.validate(b.build()));
    assertThat(validator.validate(b.setName("foo@bar").build()),
               contains(
                   equalTo("Job name may only contain [0-9a-zA-Z-_.] in job name [foo@bar].")));
    assertThat(validator.validate(b.setName("foo&bar").build()),
               contains(
                   equalTo("Job name may only contain [0-9a-zA-Z-_.] in job name [foo&bar].")));
  }

  @Test
  public void testInvalidVersionsFail() throws Exception {
    final Job.Builder b = Job.newBuilder().setName("foo").setImage("foo");
    assertEquals(newHashSet("Job version was not specified in job id [foo:null].",
        "Job hash was not specified in job id [foo:null]."),
                 validator.validate(b.build()));
    assertThat(validator.validate(b.setVersion("17@bar").build()),
               contains(equalTo("Job version may only contain [0-9a-zA-Z-_.] "
                   + "in job version [17@bar].")));
    assertThat(validator.validate(b.setVersion("17&bar").build()),
               contains(equalTo("Job version may only contain [0-9a-zA-Z-_.] "
                   + "in job version [17&bar].")));
  }


  @Test
  public void testInvalidImagesFail() throws Exception {
    final Job.Builder b = Job.newBuilder().setName("foo").setVersion("1");

    assertEquals(newHashSet("Tag cannot be empty"),
                 validator.validate(b.setImage("repo:").build()));

    assertFalse(validator.validate(b.setImage("repo:/").build()).isEmpty());

    assertEquals(newHashSet("Invalid domain name: \"1.2.3.4.\""),
                 validator.validate(b.setImage("1.2.3.4.:4711/namespace/repo").build()));

    assertEquals(newHashSet("Invalid domain name: \" reg.istry\""),
                 validator.validate(b.setImage(" reg.istry:4711/repo").build()));

    assertEquals(newHashSet("Invalid domain name: \"reg .istry\""),
                 validator.validate(b.setImage("reg .istry:4711/repo").build()));

    assertEquals(newHashSet("Invalid domain name: \"reg.istry \""),
                 validator.validate(b.setImage("reg.istry :4711/repo").build()));

    assertEquals(newHashSet("Invalid port in endpoint: \"reg.istry: 4711\""),
                 validator.validate(b.setImage("reg.istry: 4711/repo").build()));

    assertEquals(newHashSet("Invalid port in endpoint: \"reg.istry:4711 \""),
                 validator.validate(b.setImage("reg.istry:4711 /repo").build()));

    assertEquals(newHashSet("Invalid repository name ( repo), only [a-z0-9-_.] are allowed"),
                 validator.validate(b.setImage("reg.istry:4711/ repo").build()));

    assertEquals(newHashSet("Invalid namespace name (namespace ), only [a-z0-9_] are " +
                            "allowed, size between 4 and 30"),
                 validator.validate(b.setImage("reg.istry:4711/namespace /repo").build()));

    assertEquals(newHashSet("Invalid repository name ( repo), only [a-z0-9-_.] are allowed"),
                 validator.validate(b.setImage("reg.istry:4711/namespace/ repo").build()));

    assertEquals(newHashSet("Invalid repository name (repo ), only [a-z0-9-_.] are allowed"),
                 validator.validate(b.setImage("reg.istry:4711/namespace/repo ").build()));

    assertEquals(newHashSet("Invalid domain name: \"foo-.ba|z\""),
                 validator.validate(b.setImage("foo-.ba|z/namespace/baz").build()));

    assertEquals(newHashSet("Invalid domain name: \"reg..istry\""),
                 validator.validate(b.setImage("reg..istry/namespace/baz").build()));

    assertEquals(newHashSet("Invalid domain name: \"reg..istry\""),
                 validator.validate(b.setImage("reg..istry/namespace/baz").build()));

    assertEquals(newHashSet("Invalid port in endpoint: \"foo:345345345\""),
                 validator.validate(b.setImage("foo:345345345/namespace/baz").build()));

    assertEquals(newHashSet("Invalid port in endpoint: \"foo:-17\""),
                 validator.validate(b.setImage("foo:-17/namespace/baz").build()));

    assertEquals(newHashSet("Invalid repository name (bar/baz/quux), only [a-z0-9-_.] are allowed"),
                 validator.validate(b.setImage("foos/bar/baz/quux").build()));

    assertEquals(newHashSet("Invalid namespace name (foo), only [a-z0-9_] are allowed, " +
                            "size between 4 and 30"),
                 validator.validate(b.setImage("foo/bar").build()));

    final String foos = Strings.repeat("foo", 100);
    assertEquals(newHashSet("Invalid namespace name (" + foos + "), only [a-z0-9_] are allowed, " +
                            "size between 4 and 30"),
                 validator.validate(b.setImage(foos + "/bar").build()));
  }

  @Test
  public void testInvalidVolumesFail() {
    final Job j = Job.newBuilder().setName("foo").setVersion("1").setImage("foobar").build();
    assertEquals(newHashSet("Invalid volume path: /"),
                 validator.validate(j.toBuilder().addVolume("/").build()));

    assertEquals(newHashSet("Invalid volume path: /foo:"),
                 validator.validate(j.toBuilder().addVolume("/foo:", "/bar").build()));

    assertEquals(newHashSet("Volume path is not absolute: foo"),
                 validator.validate(j.toBuilder().addVolume("foo").build()));

    assertEquals(newHashSet("Volume path is not absolute: foo"),
                 validator.validate(j.toBuilder().addVolume("foo", "/bar").build()));

    assertEquals(newHashSet("Volume source is not absolute: bar"),
                 validator.validate(j.toBuilder().addVolume("/foo", "bar").build()));
  }

  @Test
  public void testInvalidHealthCheckFail() {
    final Job jobWithNoPorts = Job.newBuilder()
        .setName("foo")
        .setVersion("1")
        .setImage("foobar")
        .setHealthCheck(HEALTH_CHECK)
        .build();

    assertEquals(1, validator.validate(jobWithNoPorts).size());

    final Job jobWithWrongPort = jobWithNoPorts.toBuilder()
        .addPort("a", PortMapping.of(1, 1))
        .build();
    assertEquals(1, validator.validate(jobWithWrongPort).size());
  }
  
  @Test
  public void testExpiry() {
    // make a date that's 24 hours behind
    final java.util.Date d = new java.util.Date(System.currentTimeMillis() - (86400 * 1000));
    final Job j = Job.newBuilder().setName("foo").setVersion("1").setImage("foobar")
        .setExpires(d).build();
    assertEquals(newHashSet("Job expires in the past"), validator.validate(j));
  }
}

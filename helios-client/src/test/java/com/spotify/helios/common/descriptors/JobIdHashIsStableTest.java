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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import java.util.Date;
import org.junit.Test;

/**
 * Verifies that the computed hash of a Job stays the same even if we add new fields to the Job.
 *
 * <p>A helios client (the CLI, a user of TemporaryJobs, etc) will construct a Job, compute it's
 * hash, and send them to the server as JSON. The server will compute the same hash of all of the
 * Job's fields and check that the hash matches.</p>
 *
 * <p>Whenever we update the server-side code, we can't be sure that all of the clients are at the
 * same version. In fact we want to avoid any changes that would require the server and (every
 * single) client to be at the same version. Therefore we have to be careful whenever adding new
 * fields to the Job (or new fields to its fields) that we do not change the computed hash that a
 * newer server would compute for a Job-and-hash sent by an older client.</p>
 *
 * <p>What this means in practice is that we cannot add new fields or sub-fields to the Job class
 * and assign default non-null or non-empty values to them within their class or declaration.
 * The JSON serialization used by the Job class when computing <code>hash = sha1(json(job))</code>
 * will not include any empty (null or empty collections) in the resulting JSON string.</p>
 *
 * <p>This test verifies that static Job instances retain the same computed hash from this point
 * forward.</p>
 */
public class JobIdHashIsStableTest {

  // a job with all of its fields (as of writing-time) set
  // don't add new fields to this Job instance! If you are trying to add a new field to the Job
  // class, instead add a new test to this class
  private static final Job JOB = Job.newBuilder()
      .addEnv("ENV", "VAR")
      .addMetadata("META", "DATA")
      .addPort("port1", PortMapping.of(80))
      .addVolume("/tmp")
      .addRegistration(ServiceEndpoint.of("svc", "tcp"), ServicePorts.of("port1"))
      .setAddCapabilities(ImmutableList.of("cap1", "cap2"))
      .setCommand(ImmutableList.of("echo", "hi"))
      .setDropCapabilities(ImmutableList.of("cap3"))
      .setExpires(new Date(1484270348))
      .setGracePeriod(10)
      .setHealthCheck(HealthCheck.newHttpHealthCheck()
          .setPath("/hello")
          .setPort("port1")
          .build()
      )
      .setHostname("hostname")
      .setImage("image:1.2.3")
      .setName("a-full-job")
      .setNetworkMode("host")
      .setRegistrationDomain("domain")
      .setResources(new Resources(1L, 2L, 3L, "4"))
      .setSecondsToWaitBeforeKill(99)
      .setSecurityOpt(ImmutableList.of("opt"))
      .setToken("token")
      .setVersion("1")
      .build();

  private static void assertExpectedHash(final String expectedHash, final Job job) {
    assertEquals("If this hash changed, you are probably introducing a "
                 + "backwards-incompatible change to the Job class!",
        expectedHash, job.getId().getHash()
    );
  }

  @Test
  public void testInitialFullJob() {
    // don't change these hard-coded hashes unless you are really sure you want to
    // break compatibility
    assertExpectedHash("9693b4579ac48ed41f317ecf5edc5496b005b79d", JOB);
  }

  @Test
  public void testMinimalJob() {
    final Job job = Job.newBuilder()
        .setName("foo")
        .setVersion("bar")
        .setImage("foo/bar:1")
        .build();

    assertExpectedHash("3f5c6e4aef30e1ab68931dbd66a83226410b87e9", job);
  }
}

/*-
 * -\-\-
 * Helios System Tests
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

package com.spotify.helios.system;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThat;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;
import java.util.Collections;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JobCreateTest extends SystemTestBase {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testInvalidHostname() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Create job
    final String output = createJobRawOutput(Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setHostname("$%^&")
        .build());
    assertThat(output, Matchers.containsString("Invalid hostname "));
  }

  @Test
  public void testPortMappingWithIp() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Create job with a portmapping that has an IP. Check it doesn't throw.
    createJobRawOutput(Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setPorts(Collections.singletonMap("foo", PortMapping.builder()
            .ip("127.0.0.1")
            .internalPort(80)
            .externalPort(80)
            .build()
        )).build());
  }

  @Test
  public void testPortMappingWithBadIp() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    awaitHostStatus(testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("foobar is not a valid IP address.");

    // Create job with a portmapping that has a bad IP. Check it throws.
    createJobRawOutput(Job.newBuilder()
        .setName(testJobName)
        .setVersion(testJobVersion)
        .setImage(BUSYBOX)
        .setPorts(Collections.singletonMap("foo", PortMapping.builder()
            .ip("foobar")
            .internalPort(80)
            .externalPort(80)
            .build()
        )).build());
  }
}

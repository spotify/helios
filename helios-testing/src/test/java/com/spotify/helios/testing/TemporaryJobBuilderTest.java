/*
 * Copyright (c) 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TemporaryJobBuilderTest {

  @Test
  public void testBuildFromJob() {
    final Job job = Job.newBuilder()
        .setName("foo")
        .addPort("http", PortMapping.of(8080))
        .build();

    final Deployer deployer = mock(Deployer.class);
    final Prober prober = mock(Prober.class);
    final Map<String, String> env = Collections.emptyMap();
    final TemporaryJobReports.ReportWriter reportWriter =
        mock(TemporaryJobReports.ReportWriter.class);

    final TemporaryJobBuilder temporaryJobBuilder =
        new TemporaryJobBuilder(deployer, "prefix-", prober, env, reportWriter, job.toBuilder());

    final ImmutableList<String> hosts = ImmutableList.of("host1");

    temporaryJobBuilder.deploy(hosts);

    final ImmutableSet<String> expectedWaitPorts = ImmutableSet.of("http");
    verify(deployer).deploy(any(Job.class), eq(hosts), eq(expectedWaitPorts),
                            eq(prober), eq(reportWriter));
  }
}

/*
 * Copyright (c) 2014 Spotify AB.
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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SoloAgentProberTest {

  private static final String HOST = "host";

  @Test
  public void testSuccess() throws Exception {
    final HeliosClient client = mock(HeliosClient.class);
    final List<String> hosts = ImmutableList.of(HOST);
    when(client.listHosts()).thenReturn(Futures.immediateFuture(hosts));

    final Map<String, HostStatus> hostStatuses =
        ImmutableMap.of(HOST, HostStatus.newBuilder().setStatus(HostStatus.Status.UP)
            .setJobs(Collections.<JobId, Deployment>emptyMap())
            .setStatuses(Collections.<JobId, TaskStatus>emptyMap())
            .build());
    when(client.hostStatuses(hosts)).thenReturn(Futures.immediateFuture(hostStatuses));

    final Boolean check = new SoloAgentProber().check(client);
    assertTrue(check);
  }

  @Test
  public void testNoHosts() throws Exception {
    final HeliosClient client = mock(HeliosClient.class);
    when(client.listHosts()).thenReturn(Futures.immediateFuture(Collections.<String>emptyList()));

    final Boolean check = new SoloAgentProber().check(client);
    assertThat(check, equalTo(null));
  }

  @Test
  public void testNoHostStatuses() throws Exception {
    final HeliosClient client = mock(HeliosClient.class);
    final List<String> hosts = ImmutableList.of(HOST);
    when(client.listHosts()).thenReturn(Futures.immediateFuture(hosts));

    when(client.hostStatuses(hosts)).thenReturn(Futures.immediateFuture(
        Collections.<String, HostStatus>emptyMap()));

    final Boolean check = new SoloAgentProber().check(client);
    assertThat(check, equalTo(null));
  }

  @Test
  public void testNoHostsUp() throws Exception {
    final HeliosClient client = mock(HeliosClient.class);
    final List<String> hosts = ImmutableList.of(HOST);
    when(client.listHosts()).thenReturn(Futures.immediateFuture(hosts));

    final Map<String, HostStatus> hostStatuses =
        ImmutableMap.of(HOST, HostStatus.newBuilder().setStatus(HostStatus.Status.DOWN)
            .setJobs(Collections.<JobId, Deployment>emptyMap())
            .setStatuses(Collections.<JobId, TaskStatus>emptyMap())
            .build());
    when(client.hostStatuses(hosts)).thenReturn(Futures.immediateFuture(hostStatuses));

    final Boolean check = new SoloAgentProber().check(client);
    assertThat(check, equalTo(null));
  }
}

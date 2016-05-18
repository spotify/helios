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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.HostStatus.Builder;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO (dxia) Write tests that make sense for DefaultDeployer.
public class DefaultDeployerTest {
  private static final String HOSTB = "hostb";
  private static final String HOSTA = "hosta";

  private static final List<TemporaryJob> EMPTY_JOBS_LIST = Lists.newArrayList();
  private static final ListenableFuture<HostStatus> DOWN_STATUS = Futures.immediateFuture(
      makeDummyStatusBuilder().setStatus(DOWN).build());
  private static final ListenableFuture<HostStatus> UP_STATUS = Futures.immediateFuture(
      makeDummyStatusBuilder().setStatus(UP).build());
  private static final List<String> HOSTS = ImmutableList.of(HOSTA, HOSTB);
  private static final long TIMEOUT = MINUTES.toMillis(5);
  
  @Rule
  public ExpectedException exception = ExpectedException.none();

  private final HeliosClient client = mock(HeliosClient.class);

  private final DefaultDeployer deployer =
      new DefaultDeployer(client, EMPTY_JOBS_LIST, "", TIMEOUT);

  @Ignore
  @Test
  public void testTryAgainOnHostDown() throws Exception {
    // hosta is down, hostb is up.
    when(client.hostStatus(HOSTA)).thenReturn(DOWN_STATUS);
    when(client.hostStatus(HOSTB)).thenReturn(UP_STATUS);
    
//    assertEquals(HOSTB, deployer.pickHost(HOSTS));
  }

  @Ignore
  @Test
  public void testFailsOnAllDown() throws Exception {
    final DefaultDeployer sut = new DefaultDeployer(client, EMPTY_JOBS_LIST, "", TIMEOUT);

    // hosta is down, hostb is down too.
    when(client.hostStatus(HOSTA)).thenReturn(DOWN_STATUS);
    when(client.hostStatus(HOSTB)).thenReturn(DOWN_STATUS);

    exception.expect(AssertionError.class);

//    sut.pickHost(HOSTS);
  }

  private static Builder makeDummyStatusBuilder() {
    final Map<JobId, Deployment> jobs = emptyMap();
    final Map<JobId, TaskStatus> statuses = emptyMap();
    return HostStatus.newBuilder()
        .setStatuses(statuses)
        .setJobs(jobs);
  }

  @Ignore
  @Test
  public void testHostStatusIsNull() throws Exception {
    when(client.hostStatus(HOSTA)).thenReturn(Futures.<HostStatus>immediateFuture(null));
    when(client.hostStatus(HOSTB)).thenReturn(UP_STATUS);

//    assertEquals(HOSTB, deployer.pickHost(HOSTS));
  }
}

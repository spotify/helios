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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;

import static com.spotify.helios.common.descriptors.HostStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DefaultDeployerTest {
  private static final String HOSTB = "hostb";
  private static final String HOSTA = "hosta";

  private static final List<TemporaryJob> EMPTY_JOBS_LIST = Lists.newArrayList();
  private static final ListenableFuture<HostStatus> DOWN_STATUS = Futures.immediateFuture(
      makeDummyStatusBuilder().setStatus(DOWN).build());
  private static final ListenableFuture<HostStatus> UP_STATUS = Futures.immediateFuture(
      makeDummyStatusBuilder().setStatus(UP).build());
  private static final List<String> HOSTS = ImmutableList.of(HOSTA, HOSTB);
  
  /** Pick the first host in the list */
  private static final HostPickingStrategy PICK_FIRST = new HostPickingStrategy() {
    @Override
    public String pickHost(final List<String> hosts) {
      return hosts.get(0);
    }
  };
  
  @Mock private HeliosClient client;
  
  @Test
  public void testTryAgainOnHostDown() throws Exception {
    final DefaultDeployer sut = new DefaultDeployer(client, EMPTY_JOBS_LIST, PICK_FIRST, "");
    
    // hosta is down, hostb is up. 
    when(client.hostStatus(HOSTA)).thenReturn(DOWN_STATUS);
    when(client.hostStatus(HOSTB)).thenReturn(UP_STATUS);
    
    assertEquals(HOSTB, sut.pickHost(HOSTS));
  }

  @Test
  public void testFailsOnAllDown() throws Exception {
    final DefaultDeployer sut = new DefaultDeployer(client, EMPTY_JOBS_LIST, PICK_FIRST, "");
    
    // hosta is down, hostb is down too. 
    when(client.hostStatus(HOSTA)).thenReturn(DOWN_STATUS);
    when(client.hostStatus(HOSTB)).thenReturn(DOWN_STATUS);
    
    try {
      sut.pickHost(HOSTS);
      throw new RuntimeException("Didn't throw exception!?");
    } catch (AssertionError e) {
      // OK!
    } catch (RuntimeException e) {
      if (e.getMessage().startsWith("Didn't")) {
        fail("should have thrown assertion failure");
      }
    }
  }

  private static Builder makeDummyStatusBuilder() {
    final Map<JobId, Deployment> jobs = emptyMap();
    final Map<JobId, TaskStatus> statuses = emptyMap();
    final Builder builder = HostStatus.newBuilder()
        .setStatuses(statuses)
        .setJobs(jobs);
    return builder;
  }
}

/*
 * Copyright (c) 2015 Spotify AB.
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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;

import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static java.lang.String.format;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultUndeployerTest {

  private HeliosClient client;

  private static final String HOST1 = "HOST1";
  private static final String HOST2 = "HOST2";
  private static final List<String> HOSTS = ImmutableList.of(HOST1, HOST2);
  private static final JobId JOB_ID = new JobId("foo", "1", "hash");
  private static final Job JOB = Job.newBuilder().setName("foo").setVersion("1").build();

  @Before
  public void before() {
    client = mock(HeliosClient.class);
  }

  @Test
  public void testSuccess() throws Exception {
    final ListenableFuture<JobUndeployResponse> undeployFuture1 = Futures
        .immediateFuture(new JobUndeployResponse(JobUndeployResponse.Status.OK, HOST1, JOB_ID));
    final ListenableFuture<JobUndeployResponse> undeployFuture2 = Futures
        .immediateFuture(new JobUndeployResponse(JobUndeployResponse.Status.OK, HOST2, JOB_ID));

    //noinspection unchecked
    when(client.undeploy(argThat(matchesNameAndVersion(JOB_ID)), anyString())).thenReturn(
        undeployFuture1, undeployFuture2);

    when(client.deleteJob(argThat(matchesNameAndVersion(JOB_ID)))).thenReturn(
        Futures.immediateFuture(new JobDeleteResponse(JobDeleteResponse.Status.OK)));

    final List<AssertionError> errors = new DefaultUndeployer(client).undeploy(JOB, HOSTS);

    assertThat(errors, Matchers.empty());
  }

  @Test
  public void testUndeployError() throws Exception {
    final ListenableFuture<JobUndeployResponse> undeployFuture1 = Futures.immediateFuture(
        new JobUndeployResponse(JobUndeployResponse.Status.FORBIDDEN, HOST1, JOB_ID));
    final ListenableFuture<JobUndeployResponse> undeployFuture2 = Futures.immediateFuture(
        new JobUndeployResponse(JobUndeployResponse.Status.HOST_NOT_FOUND, HOST2, JOB_ID));

    //noinspection unchecked
    when(client.undeploy(argThat(matchesNameAndVersion(JOB_ID)), anyString())).thenReturn(
        undeployFuture1, undeployFuture2);

    when(client.deleteJob(argThat(matchesNameAndVersion(JOB_ID)))).thenReturn(
        Futures.immediateFuture(new JobDeleteResponse(JobDeleteResponse.Status.OK)));

    final List<AssertionError> errors = new DefaultUndeployer(client).undeploy(JOB, HOSTS);

    assertThat(errors, hasSize(2));
  }

  @Test
  public void testDeleteError() throws Exception {
    final ListenableFuture<JobUndeployResponse> undeployFuture1 = Futures.immediateFuture(
        new JobUndeployResponse(JobUndeployResponse.Status.OK, HOST1, JOB_ID));
    final ListenableFuture<JobUndeployResponse> undeployFuture2 = Futures.immediateFuture(
        new JobUndeployResponse(JobUndeployResponse.Status.OK, HOST2, JOB_ID));

    //noinspection unchecked
    when(client.undeploy(argThat(matchesNameAndVersion(JOB_ID)), anyString())).thenReturn(
        undeployFuture1, undeployFuture2);

    when(client.deleteJob(argThat(matchesNameAndVersion(JOB_ID)))).thenReturn(
        Futures.immediateFuture(new JobDeleteResponse(JobDeleteResponse.Status.STILL_IN_USE)));

    final List<AssertionError> errors = new DefaultUndeployer(client).undeploy(JOB, HOSTS);

    assertThat(errors.size(), equalTo(1));
  }

  private CustomTypeSafeMatcher<JobId> matchesNameAndVersion(final JobId jobId) {
    return new CustomTypeSafeMatcher<JobId>(
        format("A JobId with name %s and version %s", jobId.getName(), jobId.getVersion())) {
      @Override
      protected boolean matchesSafely(final JobId item) {
        return item.getName().equals(jobId.getName()) &&
               item.getVersion().equals(jobId.getVersion());
      }
    };
  }
}

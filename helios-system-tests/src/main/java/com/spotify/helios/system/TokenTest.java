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

package com.spotify.helios.system;

import com.google.common.collect.Lists;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.SetGoalResponse;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TokenTest extends SystemTestBase {

  private static final String TOKEN = "--token=abc123";
  private static final String NO_TOKEN = null;
  private static final String WRONG_TOKEN = "--token=wrongToken";

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(testHost());
    final HeliosClient client = defaultClient();
    awaitHostRegistered(client, testHost(), LONG_WAIT_SECONDS, SECONDS);
    awaitHostStatus(client, testHost(), UP, LONG_WAIT_SECONDS, SECONDS);

    // Create a job and specify a token
    final CreateJobResponse createJobResponse = cliJson(
        CreateJobResponse.class, "create", TOKEN, testJobNameAndVersion, BUSYBOX);
    assertThat(createJobResponse.getStatus(), equalTo(CreateJobResponse.Status.OK));

    // Now run all operations which honor the token. Test that they work as
    // expected when given no token, the wrong token, and the correct token.

    deploy(NO_TOKEN, JobDeployResponse.Status.FORBIDDEN);
    deploy(WRONG_TOKEN, JobDeployResponse.Status.FORBIDDEN);
    deploy(TOKEN, JobDeployResponse.Status.OK);

    stop(NO_TOKEN, SetGoalResponse.Status.FORBIDDEN);
    stop(WRONG_TOKEN, SetGoalResponse.Status.FORBIDDEN);
    stop(TOKEN, SetGoalResponse.Status.OK);

    undeploy(NO_TOKEN, JobUndeployResponse.Status.FORBIDDEN);
    undeploy(WRONG_TOKEN, JobUndeployResponse.Status.FORBIDDEN);
    undeploy(TOKEN, JobUndeployResponse.Status.OK);

    remove(NO_TOKEN, JobDeleteResponse.Status.FORBIDDEN);
    remove(WRONG_TOKEN, JobDeleteResponse.Status.FORBIDDEN);
    remove(TOKEN, JobDeleteResponse.Status.OK);
  }

  private void deploy(final String token, final JobDeployResponse.Status status)
      throws Exception {
    final List<String> args = buildArgs(token, testJobNameAndVersion, testHost());
    final JobDeployResponse response = cliJson(JobDeployResponse.class, "deploy", args);
    assertThat(response.getStatus(), equalTo(status));
  }

  private void stop(final String token, final SetGoalResponse.Status status)
      throws Exception {
    final List<String> args = buildArgs(token, testJobNameAndVersion, testHost());
    final SetGoalResponse response = cliJson(SetGoalResponse.class, "stop", args);
    assertThat(response.getStatus(), equalTo(status));
  }

  private void undeploy(final String token, final JobUndeployResponse.Status status)
      throws Exception {
    final List<String> args = buildArgs(token, testJobNameAndVersion, testHost());
    final JobUndeployResponse response = cliJson(JobUndeployResponse.class, "undeploy", args);
    assertThat(response.getStatus(), equalTo(status));
  }

  private void remove(final String token, final JobDeleteResponse.Status status)
      throws Exception {
    final List<String> args = buildArgs(token, "--yes", testJobNameAndVersion);
    final JobDeleteResponse response = cliJson(JobDeleteResponse.class, "remove", args);
    assertThat(response.getStatus(), equalTo(status));
  }

  private List<String> buildArgs(final String token, String... args) {
    return token == null ? Arrays.asList(args) : Lists.asList(token, args);
  }

}

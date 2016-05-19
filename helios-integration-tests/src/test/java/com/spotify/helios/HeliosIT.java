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

package com.spotify.helios;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.Utils.AgentStatusProber;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.testing.TemporaryJob;
import com.spotify.helios.testing.TemporaryJobBuilder;
import com.spotify.helios.testing.TemporaryJobs;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static com.spotify.helios.Utils.agentImage;
import static com.spotify.helios.Utils.masterImage;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class HeliosIT {

  @ClassRule
  public static final TemporaryJobs TEMPORARY_JOBS = TemporaryJobs.create();

  private static final String TEST_USER = "HeliosIT";
  private static final String TEST_HOST = "test-host";

  private String masterEndpoint;

  @Before
  public void setup() throws Exception {
    // zookeeper
    final TemporaryJob zk = TEMPORARY_JOBS.job()
        .image("jplock/zookeeper:3.4.5")
        .port("zk", 2181)
        .deploy();

    final String zkEndpoint = zk.address("zk").toString();

    // helios master
    final TemporaryJob master = TEMPORARY_JOBS.job()
        .image(masterImage())
        .port("helios", 5801)
        .command("--zk", zkEndpoint)
        .deploy();

    masterEndpoint = "http://" + master.address("helios").toString();

    final ImmutableList.Builder<String> args = new ImmutableList.Builder<String>()
        .add("--zk")
        .add(zkEndpoint)
        .add("--docker")
        .add(System.getenv("DOCKER_HOST"))
        .add("--name")
        .add(TEST_HOST);

    final String certPath = System.getenv("DOCKER_CERT_PATH");
    if (certPath != null) {
      args.add("--docker-cert-path=/certs");
    }

    // helios agent
    final TemporaryJobBuilder agent = TEMPORARY_JOBS.job()
        .image(agentImage())
        .prober(new AgentStatusProber(masterEndpoint, TEST_USER, TEST_HOST))
        .port("agent", 8080) // need to expose fake port just so prober gets invoked
        .command(args.build());

    if (certPath != null) {
      agent.volume("/certs", certPath);
    }

    agent.deploy();
  }

  @Test
  public void test() throws Exception {
    final CreateJobResponse create = cli(CreateJobResponse.class, "create", "test:1",
                                         "busybox:latest");
    assertThat(create.getStatus(), equalTo(CreateJobResponse.Status.OK));

    final JobDeployResponse deploy = cli(JobDeployResponse.class, "deploy", "test:1", TEST_HOST);
    assertThat(deploy.getStatus(), equalTo(JobDeployResponse.Status.OK));

    final JobUndeployResponse undeploy = cli(JobUndeployResponse.class,
                                             "undeploy", "--yes", "test:1", "-a");
    assertThat(undeploy.getStatus(), equalTo(JobUndeployResponse.Status.OK));

    final JobDeleteResponse delete = cli(JobDeleteResponse.class, "remove", "--yes", "test:1");
    assertThat(delete.getStatus(), equalTo(JobDeleteResponse.Status.OK));
  }

  private <T> T cli(final Class<T> klass, final String... args) throws Exception {
    return Utils.cli(klass, masterEndpoint, args);
  }

}

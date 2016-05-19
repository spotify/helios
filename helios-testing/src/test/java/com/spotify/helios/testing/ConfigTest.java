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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.spotify.helios.testing.TemporaryJobsTestBase.temporaryJobsBuilder;
import static java.lang.String.format;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConfigTest {

  private static TestParameters parameters;

  @Mock
  private static HeliosClient client;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  private static HeliosClient.Builder clientBuilder;

  @Before
  public void setup() {
    final ListenableFuture<Map<JobId, Job>> future =
        immediateFuture((Map<JobId, Job>) new HashMap<JobId, Job>());

    // Return an empty job list to skip trying to remove old jobs
    when(client.jobs()).thenReturn(future);
  }

  public static class ProfileTest implements Deployer {

    // Local is the default profile, so don't specify it explicitly to test default loading
    @Rule
    public final TemporaryJobs temporaryJobs = parameters.builder
        .client(client)
        .deployer(this)
        .build();

    @Before
    public void setup() {
      // This job will get deployed test-host
      temporaryJobs.job().deploy("test-host");

      // this job will get deployed using the host filter in the conf file
      temporaryJobs.job().deploy();
    }

    @Test
    public void test() throws Exception {
      // Dummy test so junit doesn't complain.
    }

    @Override
    public TemporaryJob deploy(Job job, List<String> hosts, Set<String> waitPorts, Prober prober) {
      // This is called when the first job is deployed
      assertThat(hosts, equalTo((List<String>) newArrayList("test-host")));
      parameters.validate(job, temporaryJobs.prefix());
      return null;
    }

    @Override
    public TemporaryJob deploy(Job job, String hostFilter, Set<String> waitPorts, Prober prober) {
      // This is called when the second job is deployed
      assertThat(hostFilter, equalTo(parameters.hostFilter));
      parameters.validate(job, temporaryJobs.prefix());
      return null;
    }

    @Override
    public void readyToDeploy() {
    }
  }

  @Test
  public void testLocalProfile() throws Exception {
    final TestParameters.JobValidator validator = new TestParameters.JobValidator() {
      @Override
      public void validate(Job job, String prefix) {
        final String local = prefix + ".local.";
        final Map<String, String> map = ImmutableMap.of(
            "SPOTIFY_TEST_THING", format("See, we used the prefix here -->%s<--", prefix),
            "SPOTIFY_DOMAIN", local,
            "SPOTIFY_POD", local);

        assertThat(job.getEnv(), equalTo(map));
        assertThat(job.getImage(), equalTo("busybox:latest"));
      }
    };

    // The local profile is the default, so we don't specify it explicitly so we can test
    // the default loading mechanism.
    parameters = new TestParameters(temporaryJobsBuilder(),
                                    ".*", validator);
    assertThat(testResult(ProfileTest.class), isSuccessful());
  }

  @Test
  public void testHeliosCiProfile() throws Exception {
    final TestParameters.JobValidator validator = new TestParameters.JobValidator() {
      @Override
      public void validate(Job job, String prefix) {
        final String domain = prefix + ".services.helios-ci.cloud.spotify.net";
        final Map<String, String> map = ImmutableMap.of(
            "SPOTIFY_DOMAIN", domain,
            "SPOTIFY_POD", domain,
            "SPOTIFY_SYSLOG_HOST", "10.99.0.1");
        assertThat(job.getEnv(), equalTo(map));
        assertThat(job.getImage(), equalTo("busybox:latest"));

      }
    };

    // Specify the helios-ci profile explicitly, but make sure that the construction of the
    // HeliosClient used by TemporaryJobs is mocked out to avoid attempting to connect to
    // possibly-unresolvable hosts.
    doReturn(client).when(clientBuilder).build();
    final TemporaryJobs.Builder builder =
        TemporaryJobs.builder("helios-ci", Collections.<String, String>emptyMap(), clientBuilder);
    parameters = new TestParameters(builder, ".+\\.helios-ci\\.cloud", validator);
    assertThat(testResult(ProfileTest.class), isSuccessful());
  }

  /**
   * Helper class allows us to inject different test parameters into ProfileTest so we can
   * reuse it to test multiple profiles.
   */
  private static class TestParameters {
    private final TemporaryJobs.Builder builder;
    private final String hostFilter;
    private final JobValidator jobValidator;

    private TestParameters(TemporaryJobs.Builder builder, String hostFilter,
                           JobValidator jobValidator) {
      this.builder = builder;
      this.hostFilter = hostFilter;
      this.jobValidator = jobValidator;
    }

    private void validate(final Job job, final String prefix) {
      jobValidator.validate(job, prefix);
    }

    private interface JobValidator {
      void validate(Job job, String prefix);
    }
  }
}

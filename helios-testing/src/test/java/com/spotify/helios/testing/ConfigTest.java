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

import com.spotify.helios.common.descriptors.Job;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

// TODO (dxia) Does this test still make sense in its current form?
public class ConfigTest {

  private static TestParameters parameters;

  public static class ProfileTest implements Deployer {

    // Local is the default profile, so don't specify it explicitly to test default loading
    @Rule
    public final TemporaryJobs temporaryJobs = parameters.builder
        .build();

    @Before
    public void setup() {
      temporaryJobs.job().deploy();
    }

    @Test
    public void test() throws Exception {
      // Dummy test so junit doesn't complain.
    }

    @Override
    public TemporaryJob deploy(Job job, Set<String> waitPorts, Prober prober,
                               TemporaryJobReports.ReportWriter reportWriter) {
      // This is called when the second job is deployed
      parameters.validate(job);
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
      public void validate(final Job job) {
        final String local = ".local.";
        final Map<String, String> map = ImmutableMap.of(
            "SPOTIFY_DOMAIN", local,
            "SPOTIFY_POD", local);

        assertThat(job.getEnv(), equalTo(map));
        assertThat(job.getImage(), equalTo("busybox:latest"));
      }
    };

    // The local profile is the default, so we don't specify it explicitly so we can test
    // the default loading mechanism.
    parameters = new TestParameters(TemporaryJobs.builder(Collections.<String, String>emptyMap()),
                                    validator);
    assertThat(testResult(ProfileTest.class), isSuccessful());
  }

  /**
   * Helper class allows us to inject different test parameters into ProfileTest so we can
   * reuse it to test multiple profiles.
   */
  private static class TestParameters {
    private final TemporaryJobs.Builder builder;
    private final JobValidator jobValidator;

    private TestParameters(TemporaryJobs.Builder builder,
                           JobValidator jobValidator) {
      this.builder = builder;
      this.jobValidator = jobValidator;
    }

    private void validate(final Job job) {
      jobValidator.validate(job);
    }

    private interface JobValidator {
      void validate(Job job);
    }
  }
}

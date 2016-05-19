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

import com.google.common.io.Resources;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JobWithConfigTest {

  @Mock
  private static HeliosClient client;

  @Mock
  private static Deployer deployer;

  @Before
  public void setup() {
    final ListenableFuture<Map<JobId, Job>> future =
        immediateFuture((Map<JobId, Job>) new HashMap<JobId, Job>());

    // Return an empty job list to skip trying to remove old jobs
    when(client.jobs()).thenReturn(future);
  }

  @Test
  public void test() throws Exception {
    assertThat(testResult(JobWithConfigTestImpl.class), isSuccessful());
  }

  public static class JobWithConfigTestImpl {

    // Local is the default profile, so don't specify it explicitly to test default loading
    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .client(client)
        .deployer(deployer)
        .build();

    @Test
    public void testJobWithConfig() throws Exception {
      final String configFile = Resources.getResource("helios_job_config.json").getPath();
      final TemporaryJobBuilder builder = temporaryJobs.jobWithConfig(configFile)
          .port("https", 443);
      builder.deploy("test-host");

      final ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
      verify(deployer).deploy(captor.capture(), anyListOf(String.class), anySetOf(String.class),
                              any(Prober.class));

      assertEquals(80, captor.getValue().getPorts().get("http").getInternalPort());
      assertEquals(443, captor.getValue().getPorts().get("https").getInternalPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJobWithBadConfig() throws Exception {
      temporaryJobs.jobWithConfig("/dev/null/doesnt_exist.json");
    }
  }
}

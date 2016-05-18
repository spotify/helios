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

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.spotify.helios.system.SystemTestBase.BUSYBOX;
import static com.spotify.helios.system.SystemTestBase.IDLE_COMMAND;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JobWithConfigTest {

  @Mock
  private static HeliosClient client;

  @Mock
  private static Deployer deployer;

  @Mock
  private static Prober prober;

  @Test
  public void test() throws Exception {
    when(prober.probe(anyString(), any(PortMapping.class))).thenReturn(true);
    assertThat(testResult(JobWithConfigTestImpl.class), isSuccessful());
  }

  public static class JobWithConfigTestImpl {

    // Local is the default profile, so don't specify it explicitly to test default loading
    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .prober(prober)
        .deployer(deployer)
        .build();

    @Test
    public void testJobWithConfig() throws Exception {
      final String configFile = Resources.getResource("helios_job_config.json").getPath();
      final TemporaryJobBuilder builder = temporaryJobs.jobWithConfig(configFile)
          .port("https", 443);
      builder.deploy();

      final ArgumentCaptor<Job> captor = ArgumentCaptor.forClass(Job.class);
      verify(deployer).deploy(captor.capture(), anySetOf(String.class),
                              any(Prober.class), any(TemporaryJobReports.ReportWriter.class));

      assertEquals(80, captor.getValue().getPorts().get("http").getInternalPort());
      assertEquals(443, captor.getValue().getPorts().get("https").getInternalPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJobWithBadConfig() throws Exception {
      temporaryJobs.jobWithConfig("/dev/null/doesnt_exist.json");
    }
  }
}

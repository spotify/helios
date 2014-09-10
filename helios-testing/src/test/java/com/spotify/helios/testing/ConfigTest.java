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
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ConfigTest {

  @Mock
  private static HeliosClient client;

  @Before
  public void setup() {
    final ListenableFuture<Map<JobId, Job>> future =
        immediateFuture((Map<JobId, Job>)new HashMap<JobId, Job>());

    // Return an empty job list to skip trying to remove old jobs
    when(client.jobs()).thenReturn(future);
  }

  public static class ValuesTest implements TemporaryJob.Deployer {

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder("valuesTest")
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
    public TemporaryJob deploy(Job job, List<String> hosts, Set<String> waitPorts) {
      // This is called when the first job is deployed
      assertThat(hosts, equalTo((List)newArrayList("test-host")));
      assertJobCorrect(job);
      return null;
    }

    @Override
    public TemporaryJob deploy(Job job, String hostFilter, Set<String> waitPorts) {
      // This is called when the second job is deployed
      assertThat(hostFilter, equalTo(".+.shared.cloud"));
      assertJobCorrect(job);
      return null;
    }

    private void assertJobCorrect(final Job job) {
      // This tests that the fields in conf file are transformed into a job correctly
      final Map<String, String> map = ImmutableMap.of("SPOTIFY_DOMAIN", "shared.cloud.spotify.net",
                                                      "SPOTIFY_SYSLOG_HOST", "10.99.0.1");
      assertThat(job.getEnv(), equalTo(map));
      assertThat(job.getImage(), equalTo("busybox"));
    }

    @Override
    public void readyToDeploy() {
    }
  }

  @Test
  public void testValues() throws Exception {
    assertThat(testResult(ValuesTest.class), isSuccessful());
  }
}

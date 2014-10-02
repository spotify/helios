package com.spotify.helios.testing;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.system.SystemTestBase;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.spotify.helios.common.descriptors.HostStatus.Status.UP;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

public class ProberTest extends SystemTestBase {

  private static HeliosClient client;
  private static String testHost;

  public static class OverrideDefaultProberTest {

    private MockProber defaultProber = new MockProber();
    private MockProber overrideProber = new MockProber();

    @Rule
    public final TemporaryJobs temporaryJobs = TemporaryJobs.builder()
        .client(client)
        .prober(defaultProber)
        .build();

    @Before
    public void setup() {
      temporaryJobs.job()
          .command(IDLE_COMMAND)
          .port("default", 4711)
          .deploy(testHost);

      temporaryJobs.job()
          .command(IDLE_COMMAND)
          .port("override", 4712)
          .prober(overrideProber)
          .deploy(testHost);
    }

    @Test
    public void test() {
      // Verify that the first job used the prober passed to the TemporaryJobs rule.
      assertThat(defaultProber.probed(), is(true));
      // Verify that the second job used the prober that was passed to its builder.
      assertThat(overrideProber.probed(), is(true));
    }
  }

  @Test
  public void testOverrideDefaultProber() throws Exception {
    startDefaultMaster();
    client = defaultClient();
    testHost = testHost();
    startDefaultAgent(testHost);
    awaitHostStatus(client, testHost, UP, LONG_WAIT_MINUTES, MINUTES);
    assertThat(testResult(OverrideDefaultProberTest.class), isSuccessful());
  }

  private static class MockProber implements Prober {
    private boolean probed;

    @Override
    public boolean probe(String host, int port) {
      return probed = true;
    }

    public boolean probed() {
      return probed;
    }
  }

}

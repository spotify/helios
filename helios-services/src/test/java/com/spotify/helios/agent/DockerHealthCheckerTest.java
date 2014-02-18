package com.spotify.helios.agent;

import com.spotify.helios.servicescommon.statistics.MeterRates;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class DockerHealthCheckerTest {
  private static final long MAX_WAIT_TIME = 3 * 1000;

  @Test
  public void test() throws Exception {
    SupervisorMetrics metrics = Mockito.mock(SupervisorMetrics.class);
    when(metrics.getDockerTimeoutRates()).thenReturn(
        new MeterRates(1, 1, 1)); // start out all is bad
    DockerHealthChecker hc = new DockerHealthChecker(metrics, TimeUnit.SECONDS, 1);
    hc.start();

    awaitHealthyState(hc, false);
    assertFalse(hc.check().isHealthy());

    when(metrics.getDockerTimeoutRates()).thenReturn(
        new MeterRates(.7, .7, .7));
    Thread.sleep(2);
    // should still be unhealthy
    assertFalse(hc.check().isHealthy());

    when(metrics.getDockerTimeoutRates()).thenReturn(
        new MeterRates(.3, .3, .3));
    awaitHealthyState(hc, true);
    assertTrue(hc.check().isHealthy());
  }

  private void awaitHealthyState(DockerHealthChecker hc, boolean state) throws Exception {
    long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < MAX_WAIT_TIME) {
      boolean healthy = hc.check().isHealthy();
      if (healthy == state) {
        return;
      }
      Thread.sleep(500);
    }
    fail("failed waiting for health state change to " + state);
  }
}

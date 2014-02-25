package com.spotify.helios.agent;

import com.aphyr.riemann.Proto;
import com.spotify.helios.servicescommon.CapturingRiemannClient;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.system.NamelessTestBase;
import com.spotify.nameless.client.Nameless;
import com.spotify.nameless.client.NamelessRegistrar;
import com.yammer.metrics.core.HealthCheck;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.spotify.helios.agent.NamelessHealthChecker.FAILURE_HIGH_WATERMARK;
import static com.spotify.helios.agent.NamelessHealthChecker.FAILURE_LOW_WATERMARK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NamelessHealthCheckerTest extends NamelessTestBase {

  private CapturingRiemannClient riemannClient;
  private NamelessHealthChecker healthCheck;

  @Before
  public void setUp() throws Exception {
    riemannClient = new CapturingRiemannClient();
    final RiemannFacade facade = new RiemannFacade(riemannClient, "HOSTNAME", "SERVICE");
    healthCheck = new NamelessHealthChecker(TimeUnit.HOURS, 1, facade);
  }

  @Test
  public void testSuccess() throws Exception {
    // no event expected since we assume we're already in a good state at startup
    assertTrue("Health check failed", getSuccessfulHealthCheck().isHealthy());
    assertTrue("Expected no events", riemannClient.getEvents().isEmpty());
  }

  @Test
  public void testFailure() throws Exception {
    // health check should fail so we expect critical alert
    assertFalse("Health check failed", getFailedHealthCheck().isHealthy());
    checkForState("critical");
  }

  @Test
  public void testSuccessToFailure() throws Exception {
    // expect no events at start when everything is ok
    assertTrue("Health check failed", getSuccessfulHealthCheck().isHealthy());
    assertTrue("Expected no events", riemannClient.getEvents().isEmpty());

    // now we should get critical alert
    assertFalse("Health check should have failed", getFailedHealthCheck().isHealthy());
    checkForState("critical");

    // state has not changed so we should not get another alert
    assertFalse("Health check should have failed", getFailedHealthCheck().isHealthy());
    assertTrue("Expected no events", riemannClient.getEvents().isEmpty());
  }

  @Test
  public void testFailureToSuccess() throws Exception {
    // start of with failure
    assertFalse("Health check should have failed", getFailedHealthCheck().isHealthy());
    checkForState("critical");

    // now everything should be ok
    assertTrue("Health check failed", getSuccessfulHealthCheck().isHealthy());
    checkForState("ok");

    // things are still ok, so shouldn't get another alert
    assertTrue("Health check failed", getSuccessfulHealthCheck().isHealthy());
    assertTrue("Expected no events", riemannClient.getEvents().isEmpty());
  }

  @Test
  public void testWaterMarks() throws Exception {
    // set the numerator and denominator rates so that the failure rate will
    // come out to be halfway between the high and low watermarks
    final double numerator = 1;
    final double denominator =
        1 / (((FAILURE_HIGH_WATERMARK - FAILURE_LOW_WATERMARK) / 2) + FAILURE_LOW_WATERMARK);

    // start with healthy check
    assertTrue("Health check failed", getSuccessfulHealthCheck().isHealthy());
    assertTrue("Expected no events", riemannClient.getEvents().isEmpty());

    // move failure rate to between watermarks, should not generate failure
    assertTrue("Health check failed", getHealthCheck(numerator, denominator).isHealthy());
    assertTrue("Expected no events", riemannClient.getEvents().isEmpty());

    // move failure rate above high watermark, should generate failure
    assertFalse("Health check should have failed", getFailedHealthCheck().isHealthy());
    checkForState("critical");

    // move failure rate between watermarks, should not generate ok
    assertFalse("Health check should have failed",
                getHealthCheck(numerator, denominator).isHealthy());
    assertTrue("Expected no events", riemannClient.getEvents().isEmpty());

    // move failure rate below low watermark, should generate ok
    assertTrue("Health check failed", getSuccessfulHealthCheck().isHealthy());
    checkForState("ok");
  }

  /**
   * The nameless alert relies on two metrics created by nameless. We read those metrics out of
   * the yammer default registry, and then alert based on their values. This test verifies that
   * nameless populates those metrics as we expect.
   * @throws Exception
   */
  @Test
  public void testMetricNames() throws Exception {
    final NamelessRegistrar registrar = Nameless.newRegistrar(namelessEndpoint);
    // Register a bogus port which will cause the heartbeat to fail. This causes nameless to
    // create and increment both metrics.
    registrar.register("namelessTest", "hm", -1).get();

    final Timer timer = NamelessHealthChecker.getHeartbeatTimer();
    final Meter meter = NamelessHealthChecker.getHeartbeatErrorMeter();

    // The above call to register(...).get() blocks until the register call has been made, not
    // until it returns. Therefore we must sleep so nameless has time to update the error metric.
    Thread.sleep(500);
    assertEquals("error timer was not incremented", 1, timer.count());
    assertEquals("error meter was not incremented", 1, meter.count());
  }

  private void checkForState(String expectedState) {
    final List<Proto.Event> events = riemannClient.getEvents();
    assertFalse(events.isEmpty());
    final Proto.Event event = events.get(0);
    try {
      assertEquals(expectedState, event.getState());
    } finally {
      riemannClient.clearEvents();
    }
  }

  public HealthCheck.Result getSuccessfulHealthCheck() throws Exception {
    // set meter to 0.01 and timer to 1.0, making a failure ratio of 0.01,
    // which should be below the low water mark and therefore successful
    return getHealthCheck(0.01, 1.0);
  }

  public HealthCheck.Result getFailedHealthCheck() throws Exception {
    // set meter to 1.0 and timer to 0.01, making a failure ratio of 100,
    // which should be above the high water mark and therefore a failure
    return getHealthCheck(1.0, 0.01);
  }

  public HealthCheck.Result getHealthCheck(double numeratorRate, double denominatorRate)
      throws Exception {
    final Meter numerator = mock(Meter.class);
    final Timer denominator = mock(Timer.class);
    when(numerator.fiveMinuteRate()).thenReturn(numeratorRate);
    when(denominator.fiveMinuteRate()).thenReturn(denominatorRate);
    return healthCheck.check(numerator, denominator);
  }

}

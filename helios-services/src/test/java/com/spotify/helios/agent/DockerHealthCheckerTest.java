package com.spotify.helios.agent;

import com.spotify.helios.servicescommon.NoOpRiemannClient;
import com.spotify.helios.servicescommon.statistics.MeterRates;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static com.spotify.helios.agent.DockerHealthChecker.FAILURE_HIGH_WATERMARK;
import static com.spotify.helios.agent.DockerHealthChecker.FAILURE_LOW_WATERMARK;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class DockerHealthCheckerTest {
  private static final MeterRates OK_RATE = makeMeter(FAILURE_LOW_WATERMARK - .01);
  private static final MeterRates BETWEEN_RATE = makeMeter(FAILURE_HIGH_WATERMARK - .01);
  private static final MeterRates RUN_RATE = makeMeter(1);
  private static final MeterRates ZERO_RATE = makeMeter(0);
  private static final MeterRates BAD_RATE = makeMeter(FAILURE_HIGH_WATERMARK + .01);

  private SupervisorMetrics metrics;
  private DockerHealthChecker checker;

  private static MeterRates makeMeter(double percentage) {
    int value = (int) (percentage * 100);
    return new MeterRates(value, value, value);
  }

  @Before
  public void setUp() throws Exception {
    metrics = Mockito.mock(SupervisorMetrics.class);
    checker = new DockerHealthChecker(metrics, TimeUnit.SECONDS, 1,
        NoOpRiemannClient.facade());
  }

  @Test
  public void testTimeouts() throws Exception {
    when(metrics.getDockerTimeoutRates()).thenReturn(BAD_RATE); // start out as many timeouts
    when(metrics.getContainersThrewExceptionRates()).thenReturn(ZERO_RATE);
    when(metrics.getSupervisorRunRates()).thenReturn(RUN_RATE);

    checker.start();

    assertFalse(checker.check().isHealthy());

    when(metrics.getDockerTimeoutRates()).thenReturn(BETWEEN_RATE);
    Thread.sleep(2);
    assertFalse(checker.check().isHealthy());

    when(metrics.getDockerTimeoutRates()).thenReturn(OK_RATE);
    assertTrue(checker.check().isHealthy());
  }

  @Test
  public void testExceptions() throws Exception {
    when(metrics.getDockerTimeoutRates()).thenReturn(ZERO_RATE);
    when(metrics.getContainersThrewExceptionRates()).thenReturn(BAD_RATE);
    when(metrics.getSupervisorRunRates()).thenReturn(RUN_RATE);

    checker.start();

    assertFalse(checker.check().isHealthy());

    when(metrics.getContainersThrewExceptionRates()).thenReturn(BETWEEN_RATE);
    assertFalse(checker.check().isHealthy());

    when(metrics.getContainersThrewExceptionRates()).thenReturn(OK_RATE);
    assertTrue(checker.check().isHealthy());
  }

  @Test
  public void testBoth() throws Exception {
    when(metrics.getDockerTimeoutRates()).thenReturn(ZERO_RATE);
    when(metrics.getContainersThrewExceptionRates()).thenReturn(BAD_RATE);
    when(metrics.getSupervisorRunRates()).thenReturn(RUN_RATE);

    checker.start();

    assertFalse(checker.check().isHealthy());

    when(metrics.getContainersThrewExceptionRates()).thenReturn(BETWEEN_RATE);
    assertFalse(checker.check().isHealthy());

    when(metrics.getContainersThrewExceptionRates()).thenReturn(OK_RATE);
    when(metrics.getDockerTimeoutRates()).thenReturn(BETWEEN_RATE); // between maintains unhealthy
    assertFalse(checker.check().isHealthy());

    when(metrics.getDockerTimeoutRates()).thenReturn(OK_RATE);
    assertTrue(checker.check().isHealthy());
  }
}

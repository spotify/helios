package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.JobId;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FlapControllerTest {
  @Mock private Clock clock;
  private JobId JOB_ID = new JobId("NAME", "VERSION", "deadbeef");

  @Test
  public void testEnterAndExitFlapping() throws Exception {
    when(clock.now()).thenReturn(
      new Instant(1),
      new Instant(2),
      new Instant(8),
      new Instant(34));

    FlapController controller = FlapController.newBuilder()
        .setJobId(JOB_ID)
        .setClock(clock)
        .setRestartCount(2)
        .setTimeRangeMillis(20)
        .setThrottleMillis(5)
        .build();

    assertFalse(controller.isFlapping());
    controller.jobDied(); // 1 second of runtime T=1
    assertFalse(controller.isFlapping());

    controller.jobDied(); // total of 2ms of runtime T=2
    assertTrue(controller.isFlapping()); // next time job would start would be at t=7 seconds

    controller.jobDied(); // total of 3ms of runtime T=8 (5 of that is throttle)
    assertTrue(controller.isFlapping()); // next time job would start would be at t=13

    controller.jobDied(); // ran 21ms additionally here, so should disengage flapping T=34
    assertFalse(controller.isFlapping());

    assertEquals(5L, controller.getThrottleMillis());
  }
}

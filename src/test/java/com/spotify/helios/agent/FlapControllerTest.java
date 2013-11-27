package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.JobId;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static com.spotify.helios.common.descriptors.ThrottleState.NO;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FlapControllerTest {
  @Mock private Clock clock;
  private JobId JOB_ID = new JobId("NAME", "VERSION", "deadbeef");

  @Test
  public void testEnterAndExitFlapping() throws Exception {
    RestartPolicy policy = RestartPolicy.newBuilder()
        .setFlappingThrottleMills(5)
        .setNormalRestartIntervalMillis(0)
        .build();

    FlapController controller = FlapController.newBuilder()
        .setJobId(JOB_ID)
        .setClock(clock)
        .setRestartCount(2)
        .setTimeRangeMillis(20)
        .setRestartPolicy(policy)
        .build();

    assertFalse(controller.isFlapping());

    when(clock.now()).thenReturn(new Instant(1));
    controller.jobDied(NO); // 1 second of runtime T=1
    assertFalse(controller.isFlapping());

    when(clock.now()).thenReturn(new Instant(2));
    controller.jobDied(NO); // total of 2ms of runtime T=2
    assertTrue(controller.isFlapping()); // next time job would start would be at t=7 seconds

    when(clock.now()).thenReturn(new Instant(8));
    controller.jobDied(NO); // total of 3ms of runtime T=8 (5 of that is throttle)
    assertTrue(controller.isFlapping()); // next time job would start would be at t=13

    when(clock.now()).thenReturn(new Instant(34));
    controller.jobDied(FLAPPING); // ran 21ms additionally here, so should disengage flapping T=34
    assertFalse(controller.isFlapping());
  }
}

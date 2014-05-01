package com.spotify.helios.agent;

import com.google.common.base.Suppliers;

import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.ImageInspectResponse;
import com.spotify.helios.common.HeliosRuntimeException;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.serviceregistration.NopServiceRegistrar;
import com.spotify.helios.servicescommon.statistics.NoopSupervisorMetrics;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static com.spotify.helios.common.descriptors.ThrottleState.NO;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TaskRunnerTest {
  private static final long MIN_UNFLAP_TIME = 10L;
  private static final String IMAGE = "spotify:17";
  private static final Job JOB = Job.newBuilder()
      .setName("foobar")
      .setCommand(asList("foo", "bar"))
      .setImage("spotify:17")
      .setVersion("4711")
      .build();
  private static final String HOST = "HOST";

  @Mock private MonitoredDockerClient mockDocker;
  @Mock private StatusUpdater statusUpdater;
  @Mock private Clock clock;

  @Test
  public void test() throws Throwable {
    final FlapController flapController = FlapController.newBuilder()
        .setJobId(JOB.getId())
        .setRestartCount(1)
        .setTimeRangeMillis(MIN_UNFLAP_TIME)
        .setClock(clock)
        .setTaskStatusManager(new FakeTaskStatusManager())
        .build();

    final TaskRunner tr = new TaskRunner(
        0,
        new NopServiceRegistrar(),
        JOB,
        new CommandWrapper() {
          @Override
          public void modifyStartConfig(HostConfig hostConfig) {
            assertNotNull(hostConfig);
          }

          @Override
          public void modifyCreateConfig(String image, Job job, ImageInspectResponse imageInfo,
              ContainerConfig createConfig) {
            assertNotNull(image);
            assertNotNull(job);
            assertNotNull(imageInfo);
            assertNotNull(createConfig);
          }
        },
        new ContainerUtil(HOST, JOB, Collections.<String, Integer>emptyMap(),
                          Collections.<String, String>emptyMap()),
        new NoopSupervisorMetrics(),
        mockDocker,
        flapController,
        new AtomicReference<ThrottleState>(NO),
        statusUpdater,
        Suppliers.ofInstance((String) null));

    when(mockDocker.safeInspectImage(IMAGE))
        .thenReturn(new ImageInspectResponse())
        .thenReturn(null);
    tr.run();
    try {
      tr.result().get();
      fail("this should throw");
    } catch (Throwable t) {
      assertTrue(t instanceof ExecutionException);
      assertEquals(HeliosRuntimeException.class, t.getCause().getClass()); // not AssertionError from above
    }
  }
}

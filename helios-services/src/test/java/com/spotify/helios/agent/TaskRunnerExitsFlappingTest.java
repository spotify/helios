package com.spotify.helios.agent;

import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.ContainerCreateResponse;
import com.kpelykh.docker.client.model.ContainerInspectResponse;
import com.kpelykh.docker.client.model.HostConfig;
import com.kpelykh.docker.client.model.ImageInspectResponse;
import com.spotify.helios.Polling;
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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.spotify.helios.common.descriptors.ThrottleState.FLAPPING;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TaskRunnerExitsFlappingTest {
  private static final long MIN_UNFLAP_TIME = 10L;
  private static final Integer CONTAINER_EXIT_CODE = 42;
  private static final String IMAGE = "spotify:17";
  private static final Job JOB = Job.newBuilder()
      .setName("foobar")
      .setCommand(asList("foo", "bar"))
      .setImage("spotify:17")
      .setVersion("4711")
      .build();
  private static final String HOST = "HOST";
  private static final String CONTAINER_ID = "CONTAINER_ID";

  @Mock private MonitoredDockerClient mockDocker;
  @Mock private StatusUpdater statusUpdater;
  @Mock private Clock clock;

  @Test
  public void test() throws Exception {
    when(clock.now()).thenReturn(new Instant(0L));
    final FakeTaskStatusManager manager = new FakeTaskStatusManager();
    // start off flapping already
    final AtomicReference<ThrottleState> throttle = new AtomicReference<ThrottleState>(FLAPPING);
    manager.updateFlappingState(true);

    final FlapController flapController = FlapController.newBuilder()
        .setJobId(JOB.getId())
        .setRestartCount(1)
        .setTimeRangeMillis(MIN_UNFLAP_TIME)
        .setClock(clock)
        .setTaskStatusManager(manager)
        .build();

    final TaskRunner tr = new TaskRunner(
        0,
        new NopServiceRegistrar(),
        JOB, new NoOpCommandWrapper(),
        new ContainerUtil(HOST, JOB, Collections.<String, Integer>emptyMap(),
                          Collections.<String, String>emptyMap()),
        new NoopSupervisorMetrics(),
        mockDocker,
        flapController,
        throttle,
        statusUpdater,
        Suppliers.ofInstance((String) null));

    when(mockDocker.safeInspectImage(IMAGE)).thenReturn(new ImageInspectResponse());
    when(mockDocker.createContainer(any(ContainerConfig.class),
        any(String.class))).thenReturn(new ContainerCreateResponse() {{
          id = "CONTAINER_ID";
        }});
    when(mockDocker.startContainer(eq(CONTAINER_ID), any(HostConfig.class)))
        .thenReturn(Futures.<Void>immediateFuture(null));
    when(mockDocker.safeInspectContainer(CONTAINER_ID)).thenReturn(new ContainerInspectResponse() {{
      state = new ContainerState() {{
        running = true;
      }};
      networkSettings = new NetworkSettings() {{
        ports = emptyMap();
      }};
    }});

    final SettableFuture<Integer> containerWaitFuture = SettableFuture.create();
    when(mockDocker.waitContainer(CONTAINER_ID)).thenReturn(containerWaitFuture);

    final CyclicBarrier startBarrier = new CyclicBarrier(2);
    final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
      Executors.newSingleThreadExecutor());

    // run this in the background
    ListenableFuture<?> trFuture = executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          startBarrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          fail("barrier failure" + e);
        }
        try {
          tr.run();
        } catch (Throwable e) {
          fail("tr threw exception " + e);
        }
      }
    });

    startBarrier.await();

    // wait for flapping state change
    assertNotNull(Polling.await(30, TimeUnit.SECONDS, new Callable<ThrottleState>() {
      @Override
      public ThrottleState call() throws Exception {
        if (manager.isFlapping()) {
          return null;
        }
        return throttle.get();
      }
    }));

    // make it so container ran for 1s, more than the 10ms req'd
    when(clock.now()).thenReturn(new Instant(MIN_UNFLAP_TIME + 1));
    // tell "container" to die
    containerWaitFuture.set(CONTAINER_EXIT_CODE);
    // wait for task runner to finish
    trFuture.get();

    assertEquals(CONTAINER_EXIT_CODE, tr.result().get());
    assertEquals(ThrottleState.NO, throttle.get());
  }
}

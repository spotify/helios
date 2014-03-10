package com.spotify.helios.agent;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class InterruptingScheduledService extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(InterruptingScheduledService.class);

  private final ThreadFactory threadFactory = new ThreadFactoryBuilder()
      .setNameFormat(serviceName() + "-%d").build();

  private final ScheduledExecutorService executorService =
      MoreExecutors.getExitingScheduledExecutorService(
          (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, threadFactory),
          0, SECONDS);

  private final Runnable runnable = new Runnable() {
    @Override
    public void run() {
      try {
        runOneIteration();
      } catch (InterruptedException e) {
        log.debug("scheduled service interrupted: {}", serviceName());
      } catch (Exception e) {
        if (e.getCause() instanceof InterruptedException) {
          log.debug("scheduled service interrupted: {}", serviceName());
        } else {
          log.warn("scheduled service threw exception: {}", serviceName(), e);
        }
      }
    }
  };

  private ScheduledFuture<?> future;

  protected abstract void runOneIteration() throws InterruptedException;

  @Override
  protected void startUp() throws Exception {
    future = schedule(runnable, executorService);
  }

  @Override
  protected void shutDown() throws Exception {
    future.cancel(true);
    executorService.shutdownNow();
    executorService.awaitTermination(1, DAYS);
  }

  protected abstract ScheduledFuture<?> schedule(Runnable runnable,
                                                 ScheduledExecutorService executorService);
}

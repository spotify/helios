/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.agent;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs {@link #runOneIteration()} on a {@link ScheduledExecutorService} (see {@link #schedule})
 * to do periodic operations.
 *
 * <p>This class is similar to {@link com.google.common.util.concurrent.AbstractScheduledService}
 * except that this class's {@link #shutDown()} method will interrupt the thread running the
 * periodic operation.
 */
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
        beforeIteration();
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

  /**
   * A hook for subclasses to insert extra steps before {@link #runOneIteration()}.
   */
  protected void beforeIteration() throws InterruptedException {}

  protected abstract void runOneIteration() throws InterruptedException;

  @Override
  protected void startUp() throws Exception {
    future = schedule(runnable, executorService);
  }

  @Override
  protected void shutDown() throws Exception {
    if (future != null) {
      future.cancel(true);
    }
    executorService.shutdownNow();
    executorService.awaitTermination(1, DAYS);
  }

  protected abstract ScheduledFuture<?> schedule(Runnable runnable,
                                                 ScheduledExecutorService executorService);
}

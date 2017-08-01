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

package com.spotify.helios.master.reaper;

import com.google.common.util.concurrent.RateLimiter;
import com.spotify.helios.agent.InterruptingScheduledService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * An abstract {@link InterruptingScheduledService} that is rate limited between defined
 * units of work within {@link #runOneIteration()}. Use this class if you need to retrieve a
 * complete set of items to process and then pause between operating on each item.
 *
 * <p>A use case is when you need to get a list of nodes from ZooKeeper and for each node run three
 * more ZooKeeper operations. You need to get all the nodes in a single {@link #runOneIteration()},
 * but you want to pause between processing each node because the fan-out of ZooKeeper operations
 * will hammer it.
 */
abstract class RateLimitedService<T> extends InterruptingScheduledService {

  private final long initialDelay;
  private final long delay;
  private final TimeUnit timeUnit;
  private final RateLimiter rateLimiter;

  RateLimitedService(final double permitsPerSecond,
                     final long initialDelay,
                     final long delay,
                     final TimeUnit timeUnit) {
    this.rateLimiter = RateLimiter.create(permitsPerSecond);
    this.initialDelay = initialDelay;
    this.delay = delay;
    this.timeUnit = timeUnit;
  }

  /**
   * Collect items to process.
   *
   * @return An iterable of T.
   */
  abstract Iterable<T> collectItems();

  /**
   * Process the item.
   *
   * @param item An instance of T.
   */
  abstract void processItem(final T item);

  @Override
  protected void runOneIteration() {
    for (final T item : collectItems()) {
      rateLimiter.acquire();
      processItem(item);
    }
  }

  @Override
  protected ScheduledFuture<?> schedule(final Runnable runnable,
                                        final ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, initialDelay, delay, timeUnit);
  }
}

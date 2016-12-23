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

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * An InterruptingScheduledService which awaits on a CountDownLatch prior to
 * {@link #runOneIteration()}. This can be used to have the Service fully enter the RUNNING state
 * and schedule it's periodic task, but block the actual execution of the task until some condition
 * is met by another thread.
 * <p>
 * Implementation note: await() is called in the thread running the "periodic task" as it is
 * easier to interrupt that thread when the Service is shut down; blocking on the latch in the
 * startUp() method caused the Service to never exit (and prevent the JVM from exiting normally
 * on SIGINT).</p>
 */
public abstract class SignalAwaitingService extends InterruptingScheduledService {

  private final CountDownLatch signal;

  public SignalAwaitingService(final CountDownLatch signal) {
    this.signal = Objects.requireNonNull(signal);
  }

  @Override
  protected void beforeIteration() throws InterruptedException {
    signal.await();
  }
}

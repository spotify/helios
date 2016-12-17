/*-
 * -\-\-
 * Helios Testing Common Library
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

package com.spotify.helios;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.RunnerScheduler;

public class Parallelized extends BlockJUnit4ClassRunner {

  private static class ThreadPoolScheduler implements RunnerScheduler {

    private ExecutorService executor;

    public ThreadPoolScheduler() {
      final String threads = System.getProperty("junit.parallel.threads", "16");
      executor = Executors.newFixedThreadPool(Integer.parseInt(threads));
    }

    @Override
    public void finished() {
      executor.shutdown();
      try {
        executor.awaitTermination(10, TimeUnit.MINUTES);
      } catch (InterruptedException exc) {
        throw new RuntimeException(exc);
      }
    }

    @Override
    public void schedule(Runnable childStatement) {
      executor.submit(childStatement);
    }
  }

  public Parallelized(Class<?> klass) throws Throwable {
    super(klass);
    setScheduler(new ThreadPoolScheduler());
  }
}

/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import static java.lang.Math.floor;

import java.util.List;
import java.util.Random;

public class HostPickingStrategies {
  private static class RandomHostPickingStrategy implements HostPickingStrategy {
    private final Random random;

    public RandomHostPickingStrategy(final Random random) {
      this.random = random;
    }

    @Override
    public String pickHost(final List<String> hosts) {
      return hosts.get(random.nextInt(hosts.size()));
    }
  }

  private static class RandomOneHostPickingStrategy implements HostPickingStrategy {
    private final double random;

    RandomOneHostPickingStrategy(final Random randomGenerator) {
      this.random = randomGenerator.nextDouble();
    }

    @Override
    public String pickHost(final List<String> hosts) {
      final Double index = floor(random * hosts.size());
      return hosts.get(index.intValue());
    }
  }

  private static Random getSeededRandom(final String key) {
    final Random randomGenerator = new Random();
    randomGenerator.setSeed(key.hashCode());
    return randomGenerator;
  }

  /**
   * For any given invocation returns a random host.
   *
   * @return The strategy object
   */
  public static HostPickingStrategy random() {
    return new RandomHostPickingStrategy(new Random());
  }

  /**
   * Pick a single host and use that for all jobs to be deployed in the tests used by this
   * strategy.  If you want multiple test classes to use the same host, share the strategy
   * between tests as a constant.
   *
   * @return The strategy object
   */
  public static HostPickingStrategy randomOneHost() {
    return new RandomOneHostPickingStrategy(new Random());
  }

  /**
   * For any given invocation returns a random host, but running the same test with the same
   * key should put jobs on the same hosts as they were the last time.
   *
   * @param key The random generator seed
   *
   * @return The strategy object
   */
  public static HostPickingStrategy deterministic(final String key) {
    return new RandomHostPickingStrategy(getSeededRandom(key));
  }

  /**
   * Deterministically, choose a single host for all jobs in the test.  That is, it will
   * choose the same host given equal values of key.  If you want multiple test classes to
   * use the same host, share the strategy between tests as a constant.
   *
   * @param key The random generator seed
   *
   * @return The strategy object
   */
  public static HostPickingStrategy deterministicOneHost(final String key) {
    return new RandomOneHostPickingStrategy(getSeededRandom(key));
  }
}

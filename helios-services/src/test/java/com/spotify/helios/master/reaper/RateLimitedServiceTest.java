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

import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class RateLimitedServiceTest {

  @Test
  public void test() throws Exception {
    final List<AtomicReference<Integer>> items = ImmutableList.of(
        new AtomicReference<>(1),
        new AtomicReference<>(1),
        new AtomicReference<>(1)
    );

    final RateLimitedService<AtomicReference<Integer>> service =
        // Rate limit to one a second
        new RateLimitedService<AtomicReference<Integer>>(1, 0, 1, TimeUnit.SECONDS) {

          @Override
          Iterable<AtomicReference<Integer>> collectItems() {
            return items;
          }

          @Override
          void processItem(final AtomicReference<Integer> item) {
            item.set(0);
          }
        };

    service.startAsync().awaitRunning();

    // After two seconds, at most one of the references should be 1.
    await().atMost(2, TimeUnit.SECONDS).until(
        () -> items.stream().filter(r -> r.get().equals(1)).count(),
        lessThanOrEqualTo(1L)
    );

    // After two more seconds, all of the references should be 0.
    await().atMost(2, TimeUnit.SECONDS).until(
        () -> items.stream().filter(r -> r.get().equals(1)).count(),
        equalTo(0L)
    );
  }
}

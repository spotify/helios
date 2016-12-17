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

package com.spotify.helios.servicescommon;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubException;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class GooglePubSubDefaultHealthCheckerTest {

  private final PubSub pubsub = mock(PubSub.class);
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
  private final GooglePubSubSender.DefaultHealthChecker checker =
      new GooglePubSubSender.DefaultHealthChecker(
          pubsub,
          "is-healthy",
          executor,
          Duration.ofDays(1));

  @After
  public void tearDown() {
    executor.shutdown();
  }

  @Test
  public void testHealthy() {
    //mock(pubsub).getTopic returns null by default
    checker.checkHealth();

    assertThat(checker.isHealthy(), is(true));
  }

  @Test
  public void testUnhealthy() {
    when(pubsub.getTopic("is-healthy"))
        .thenThrow(new PubSubException(new IOException(), false));

    checker.checkHealth();

    assertThat(checker.isHealthy(), is(false));
  }
}

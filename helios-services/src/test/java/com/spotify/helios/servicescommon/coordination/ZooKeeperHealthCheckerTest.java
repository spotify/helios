/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.servicescommon.coordination;

import com.aphyr.riemann.Proto.Event;
import com.spotify.helios.Polling;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.ZooKeeperTestingServerManager;
import com.spotify.helios.servicescommon.CapturingRiemannClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ZooKeeperHealthCheckerTest {
  private CapturingRiemannClient riemannClient;
  private ZooKeeperTestManager zk;

  @Before
  public void setUp() throws Exception {
    riemannClient = new CapturingRiemannClient();
    zk = new ZooKeeperTestingServerManager();
  }

  @After
  public void tearDown() throws Exception {
    if (zk != null) {
      zk.stop();
    }
  }

  @Test
  public void test() throws Exception {
    final DefaultZooKeeperClient client = new DefaultZooKeeperClient(zk.curatorWithSuperAuth());
    client.ensurePath("/foo/bar");
    final ZooKeeperHealthChecker hc = new ZooKeeperHealthChecker(
        client, "/foo", riemannClient.facade(), MILLISECONDS, 1);
    hc.start();

    // Start in our garden of eden where everything travaileth together in harmony....
    awaitHealthy(hc, 1, MINUTES);

    // Alas!  Behold!  Our zookeeper hath been slain with the sword of the wrath of the random!
    zk.stop();
    awaitState("critical", 1, MINUTES);

    // And lo, our zookeeper hath been resurrected and our foe vanquished!
    zk.start();
    awaitState("ok", 1, MINUTES);
    awaitHealthy(hc, 1, MINUTES);

    // And they lived happily ever after
  }

  private void awaitHealthy(final ZooKeeperHealthChecker hc, final int duration,
                            final TimeUnit timeUnit) throws Exception {
    Polling.await(duration, timeUnit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return hc.check().isHealthy() ? true : null;
      }
    });
  }

  private void awaitState(final String state, final int duration, final TimeUnit timeUnit)
      throws Exception {
    Polling.await(duration, timeUnit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final List<Event> events = riemannClient.getEvents();
        if (events.isEmpty()) {
          return null;
        }
        final Event event = events.get(0);
        return state.equals(event.getState()) ? event : null;
      }
    });
  }
}

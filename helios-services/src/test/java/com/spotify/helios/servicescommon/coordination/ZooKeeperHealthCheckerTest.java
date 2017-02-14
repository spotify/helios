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

package com.spotify.helios.servicescommon.coordination;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.spotify.helios.Polling;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.ZooKeeperTestingServerManager;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperHealthCheckerTest {
  private ZooKeeperTestManager zk;
  private ZooKeeperHealthChecker healthCheck;

  @Before
  public void setUp() throws Exception {
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

    healthCheck = new ZooKeeperHealthChecker(client);

    // Start in our garden of eden where everything travaileth together in harmony....
    awaitHealthy(1, MINUTES);

    // Alas!  Behold!  Our zookeeper hath been slain with the sword of the wrath of the random!
    zk.stop();

    // And lo, our zookeeper hath been resurrected and our foe vanquished!
    zk.start();
    awaitHealthy(1, MINUTES);

    // And they lived happily ever after
  }

  private void awaitHealthy(final int duration, final TimeUnit timeUnit) throws Exception {
    Polling.await(duration, timeUnit, () -> healthCheck.execute().isHealthy() ? true : null);
  }
}

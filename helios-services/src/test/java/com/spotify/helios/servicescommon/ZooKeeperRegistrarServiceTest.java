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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.curator.framework.state.ConnectionState.RECONNECTED;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.SettableFuture;
import com.spotify.helios.agent.BoundedRandomExponentialBackoff;
import com.spotify.helios.agent.RetryIntervalPolicy;
import com.spotify.helios.agent.Sleeper;
import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ZooKeeperRegistrarServiceTest {

  @Mock private ZooKeeperClient zkClient;
  @Mock private Listenable<ConnectionStateListener> connectionStateListenerListenable;
  @Mock private Sleeper sleeper;

  @Captor private ArgumentCaptor<ConnectionStateListener> zkClientConnectionListenerCaptor;

  private SettableFuture<Void> complete = SettableFuture.create();
  private final SettableFuture<Void> shutdownComplete = SettableFuture.create();

  private final RetryIntervalPolicy retryIntervalPolicy =
      BoundedRandomExponentialBackoff.newBuilder()
          .setMinInterval(1, MILLISECONDS)
          .setMaxInterval(30, MILLISECONDS)
          .build();

  @Before
  public void setup() {
    when(zkClient.getConnectionStateListenable()).thenReturn(connectionStateListenerListenable);
    doNothing().when(connectionStateListenerListenable)
        .addListener(zkClientConnectionListenerCaptor.capture());
  }

  /**
   * Creates a new ZooKeeperRegistrar that completes {@link #complete} when tryToRegister is
   * called and {@link #shutdownComplete} when shutDown is called..
   *
   * @param registered what the return value from
   *                   {@link ZooKeeperRegistrar#tryToRegister(ZooKeeperClient)} should be
   */
  private ZooKeeperRegistrar createStubRegistrar(boolean registered) {
    return createStubRegistrar(registered, client -> { });
  }

  /**
   * An overload of {@link #createStubRegistrar(boolean)} which invokes the Consumer in
   * tryToRegister before completing {@link #complete}. This can be used to customize the behavior
   * of tryToRegister without having to declare yet another subclass of ZooKeeperRegistrar.
   */
  private ZooKeeperRegistrar createStubRegistrar(
      boolean registered, ClientConsumer clientConsumer) {

    return new ZooKeeperRegistrar() {
      @Override
      public void startUp() throws Exception {
      }

      @Override
      public void shutDown() throws Exception {
        shutdownComplete.set(null);
      }

      @Override
      public boolean tryToRegister(ZooKeeperClient client)
          throws KeeperException, HostNotFoundException {

        clientConsumer.accept(client);

        complete.set(null);
        return registered;
      }
    };
  }

  // Would just use Consumer<ZooKeeperClient> but Consumer does not throw checked exceptions
  @FunctionalInterface
  private interface ClientConsumer {
    void accept(ZooKeeperClient client) throws KeeperException, HostNotFoundException;
  }

  @Test
  public void testAllGood() throws Exception {
    final ZooKeeperRegistrar zooKeeperRegistrar = createStubRegistrar(true);

    final CountDownLatch latch = new CountDownLatch(1);

    final ZooKeeperRegistrarService init = ZooKeeperRegistrarService.newBuilder()
        .setZooKeeperClient(zkClient)
        .setZooKeeperRegistrar(zooKeeperRegistrar)
        .setZkRegistrationSignal(latch)
        .build();

    init.startUp();

    assertNull(complete.get(3000, MILLISECONDS));

    // need to wait on latch rather than just check the count as the decrement might happen after
    // the test thread has woken up when the SettableFuture is set
    final boolean latchCleared = latch.await(3, SECONDS);
    assertTrue("Latch should be open after successful registration", latchCleared);
  }

  @Test
  public void testShutdown() throws Exception {
    final ZooKeeperRegistrar zooKeeperRegistrar = createStubRegistrar(true);

    final ZooKeeperRegistrarService init = ZooKeeperRegistrarService.newBuilder()
        .setZooKeeperClient(zkClient)
        .setZooKeeperRegistrar(zooKeeperRegistrar)
        .build();

    init.startUp();

    assertNull(complete.get(3000, MILLISECONDS));

    // if this throws exception something is bonkers
    init.shutDown();

    assertNull(shutdownComplete.get(3000, MILLISECONDS));
  }

  @Test
  public void testRetry() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);

    final ZooKeeperRegistrar zooKeeperRegistrar = createStubRegistrar(true, client -> {
      final int count = counter.incrementAndGet();
      if (count == 1) {
        throw new KeeperException.ConnectionLossException();
      }

      if (count == 2) {
        throw new HostNotFoundException("Host not found");
      }
    });

    final ZooKeeperRegistrarService init = ZooKeeperRegistrarService.newBuilder()
        .setZooKeeperClient(zkClient)
        .setZooKeeperRegistrar(zooKeeperRegistrar)
        .setRetryIntervalPolicy(retryIntervalPolicy)
        .setSleeper(sleeper)
        .build();

    init.startUp();

    assertNull(complete.get(30, SECONDS));
    assertTrue("Count must have been called at least once", counter.get() > 1);
    verify(sleeper, times(2))
        .sleep(longThat(both(greaterThanOrEqualTo(1L)).and(lessThanOrEqualTo(30L))));
  }

  @Test
  public void testReconnect() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);

    final ZooKeeperRegistrar zooKeeperRegistrar =
        createStubRegistrar(true, client -> counter.incrementAndGet());

    final ZooKeeperRegistrarService init = ZooKeeperRegistrarService.newBuilder()
        .setZooKeeperClient(zkClient)
        .setZooKeeperRegistrar(zooKeeperRegistrar)
        .setRetryIntervalPolicy(retryIntervalPolicy)
        .build();

    init.startUp();

    assertNull(complete.get(30, SECONDS));

    // simulate the reconnect
    complete = SettableFuture.create();

    final CuratorFramework curatorFramework = mock(CuratorFramework.class);
    zkClientConnectionListenerCaptor.getValue().stateChanged(curatorFramework, RECONNECTED);

    assertNull(complete.get(30, SECONDS));
    assertTrue("Count must have been called at least once", counter.get() > 1);
  }

  @Test
  public void testLatchNotSignalledOnRegistrationConflict() throws Exception {
    final ZooKeeperRegistrar zooKeeperRegistrar = createStubRegistrar(false);

    final int initialCount = 1;
    final CountDownLatch latch = new CountDownLatch(initialCount);

    final ZooKeeperRegistrarService init = ZooKeeperRegistrarService.newBuilder()
        .setZkRegistrationSignal(latch)
        .setZooKeeperClient(zkClient)
        .setZooKeeperRegistrar(zooKeeperRegistrar)
        .setRetryIntervalPolicy(retryIntervalPolicy)
        .build();

    init.startUp();

    //wait for completion
    assertNull(complete.get(3000, MILLISECONDS));

    assertEquals("Latch should not be counted down if registration did not complete",
        initialCount, latch.getCount());
  }
}

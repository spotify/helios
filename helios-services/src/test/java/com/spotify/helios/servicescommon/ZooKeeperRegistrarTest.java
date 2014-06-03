package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.SettableFuture;

import com.spotify.helios.agent.BoundedRandomExponentialBackoff;
import com.spotify.helios.agent.RetryIntervalPolicy;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.curator.framework.state.ConnectionState.RECONNECTED;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ZooKeeperRegistrarTest {

  @Mock ZooKeeperClient zkClient;
  @Mock Listenable<ConnectionStateListener> connectionStateListenerListenable;

  @Captor ArgumentCaptor<ConnectionStateListener> zkClientConnectionListenerCaptor;

  SettableFuture<Void> complete = SettableFuture.create();

  RetryIntervalPolicy retryIntervalPolicy = BoundedRandomExponentialBackoff.newBuilder()
      .setMinInterval(1, MILLISECONDS)
      .setMaxInterval(30, MILLISECONDS)
      .build();

  @Before
  public void setup() {
    when(zkClient.getConnectionStateListenable()).thenReturn(connectionStateListenerListenable);
    doNothing().when(connectionStateListenerListenable)
        .addListener(zkClientConnectionListenerCaptor.capture());
  }

  @Test
  public void testAllGood() throws Exception {

    final ZooKeeperRegistrar init = new ZooKeeperRegistrar(
        zkClient, new ZooKeeperRegistrarEventListener() {

      @Override
      public void startUp() throws Exception {
      }

      @Override
      public void shutDown() throws Exception {
      }

      @Override
      public void tryToRegister(ZooKeeperClient client) throws KeeperException {
        complete.set(null);
      }
    });

    init.startUp();

    Assert.assertNull(complete.get(3000, MILLISECONDS));
  }

  @Test
  public void testShutdown() throws Exception {

    final SettableFuture<Void> shutdownComplete = SettableFuture.create();

    final ZooKeeperRegistrar init = new ZooKeeperRegistrar(
        zkClient, new ZooKeeperRegistrarEventListener() {

      @Override
      public void startUp() throws Exception {
      }

      @Override
      public void shutDown() throws Exception {
        shutdownComplete.set(null);
      }

      @Override
      public void tryToRegister(ZooKeeperClient client) throws KeeperException {
        complete.set(null);
      }
    });

    init.startUp();

    Assert.assertNull(complete.get(3000, MILLISECONDS));

    // if this throws exception something is bonkers
    init.shutDown();

    Assert.assertNull(shutdownComplete.get(3000, MILLISECONDS));
  }

  @Test
  public void testRetry() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);

    final ZooKeeperRegistrar init = new ZooKeeperRegistrar(
        zkClient, new ZooKeeperRegistrarEventListener() {

      @Override
      public void startUp() throws Exception {

      }

      @Override
      public void shutDown() throws Exception {

      }

      @Override
      public void tryToRegister(ZooKeeperClient client) throws KeeperException {
        if (counter.incrementAndGet() == 1) { throw new KeeperException.ConnectionLossException(); }

        complete.set(null);
      }
    }, retryIntervalPolicy
    );

    init.startUp();

    Assert.assertNull(complete.get(30, SECONDS));
    Assert.assertTrue("Count must have been called at least once", counter.get() > 1);
  }

  @Test
  public void testReconnect() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);

    final ZooKeeperRegistrar init =
        new ZooKeeperRegistrar(zkClient, new ZooKeeperRegistrarEventListener() {

          @Override
          public void startUp() throws Exception {

          }

          @Override
          public void shutDown() throws Exception {

          }

          @Override
          public void tryToRegister(ZooKeeperClient client) throws KeeperException {
            counter.incrementAndGet();

            complete.set(null);

          }
        }, retryIntervalPolicy);

    init.startUp();

    Assert.assertNull(complete.get(30, SECONDS));

    // simulate the reconnect
    complete = SettableFuture.create();

    CuratorFramework curatorFramework = mock(CuratorFramework.class);
    zkClientConnectionListenerCaptor.getValue().stateChanged(curatorFramework, RECONNECTED);

    Assert.assertNull(complete.get(30, SECONDS));

    Assert.assertTrue("Count must have been called at least once", counter.get() > 1);
  }

}

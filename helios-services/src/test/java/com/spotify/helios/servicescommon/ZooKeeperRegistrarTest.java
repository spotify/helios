package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.SettableFuture;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Created by snc on 4/21/14.
 */
public class ZooKeeperRegistrarTest {

  ZooKeeperClient zkClient;
  Listenable<ConnectionStateListener> connectionStateListenerListenable;
  SettableFuture<Void> complete;

  @Captor
  ArgumentCaptor<ConnectionStateListener> zkClientConnectionListenerCaptor = ArgumentCaptor.forClass(ConnectionStateListener.class);

  @Before
  public void setup() {
    complete = SettableFuture.create();
    zkClient = mock(ZooKeeperClient.class);
    connectionStateListenerListenable = mock(Listenable.class);

    when(zkClient.getConnectionStateListenable()).thenReturn(connectionStateListenerListenable);
    doNothing().when(connectionStateListenerListenable).addListener(zkClientConnectionListenerCaptor.capture());
  }

  @Test
  public void testAllGood() throws Exception {

    final ZooKeeperRegistrar init = new ZooKeeperRegistrar(zkClient, new ZooKeeperRegistrarEventListener() {

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

    Assert.assertNull(complete.get(3000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testShutdown() throws Exception {

    final SettableFuture<Void> shutdownComplete = SettableFuture.create();

    final ZooKeeperRegistrar init = new ZooKeeperRegistrar(zkClient, new ZooKeeperRegistrarEventListener() {

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

    Assert.assertNull(complete.get(3000, TimeUnit.MILLISECONDS));

    // if this throws exception something is bonkers
    init.shutDown();

    Assert.assertNull(shutdownComplete.get(3000, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testRetry() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);

    final long maxRetryIntervalMillis = 30;

    final ZooKeeperRegistrar init = new ZooKeeperRegistrar(zkClient, new ZooKeeperRegistrarEventListener() {

      @Override
      public void startUp() throws Exception {

      }

      @Override
      public void shutDown() throws Exception {

      }

      @Override
      public void tryToRegister(ZooKeeperClient client) throws KeeperException {
        if ( counter.incrementAndGet() == 1 )
          throw new KeeperException.ConnectionLossException();

        complete.set(null);
      }
    }, 1, maxRetryIntervalMillis);

    init.startUp();

    Assert.assertNull(complete.get(maxRetryIntervalMillis * 2 + 1, TimeUnit.MILLISECONDS));
    Assert.assertTrue("Count must have been called at least once", counter.get() > 1);
  }

  @Test
  public void testReconnect() throws Exception {

    final AtomicInteger counter = new AtomicInteger(0);

    final long maxRetryIntervalMillis = 30;

    final ZooKeeperRegistrar init = new ZooKeeperRegistrar(zkClient, new ZooKeeperRegistrarEventListener() {

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
    }, 1, maxRetryIntervalMillis);

    init.startUp();

    Assert.assertNull(complete.get(maxRetryIntervalMillis * 2 + 1, TimeUnit.MILLISECONDS));

    // simulate the reconnect
    complete = SettableFuture.create();

    CuratorFramework curatorFramework = mock(CuratorFramework.class);
    zkClientConnectionListenerCaptor.getValue().stateChanged(curatorFramework, ConnectionState.RECONNECTED);

    Assert.assertNull(complete.get(maxRetryIntervalMillis * 2 + 1, TimeUnit.MILLISECONDS));

    Assert.assertTrue("Count must have been called at least once", counter.get() > 1);
  }

}

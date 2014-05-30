/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.HeliosException;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
public class MasterRespondsWithNoZKTest extends SystemTestBase {

  @Override
  protected ZooKeeperTestManager zooKeeperTestManager() {
    return new ZooKeeperTestManager() {
      @Override
      public void start() {

      }

      @Override
      public void stop() {

      }

      @Override
      public void ensure(String path) throws Exception {

      }

      @Override
      public void close() {

      }

      @Override
      public String connectString() {
        return "127.0.0.2:1024";
      }

      @Override
      public CuratorFramework curator() {
        return mock(CuratorFramework.class);
      }

      @Override
      public void awaitUp(long timeout, TimeUnit timeunit) throws TimeoutException {

      }

      @Override
      public void awaitDown(int timeout, TimeUnit timeunit) throws TimeoutException {

      }
    };
  }

  @Override
  protected void tearDownJobs() {
  }

  @Test
  public void test() throws Exception {

    startDefaultMaster();
    final HeliosClient client = defaultClient();

    try {
      client.listMasters().get().get(0);

      fail("Exception should have been thrown, as ZK doesnt exist");

    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof HeliosException);
    }

  }

}

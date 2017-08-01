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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Service.State.STOPPING;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.util.concurrent.AbstractIdleService;
import com.spotify.helios.agent.BoundedRandomExponentialBackoff;
import com.spotify.helios.agent.RetryIntervalPolicy;
import com.spotify.helios.agent.RetryScheduler;
import com.spotify.helios.agent.Sleeper;
import com.spotify.helios.agent.ThreadSleeper;
import com.spotify.helios.master.HostNotFoundException;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common logic to have agents and masters register their "up" nodes in ZK, and to keep trying if
 * ZK is down.
 */
public class ZooKeeperRegistrarService extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperRegistrarService.class);

  private final ZooKeeperClient client;
  private final ZooKeeperRegistrar zooKeeperRegistrar;
  private final Optional<CountDownLatch> zkRegistrationSignal;
  private final RetryIntervalPolicy retryIntervalPolicy;
  private final Sleeper sleeper;
  private final Reactor reactor;

  private ZooKeeperRegistrarService(final Builder builder) {
    this.client = checkNotNull(builder.zooKeeperClient);
    this.zooKeeperRegistrar = checkNotNull(builder.zooKeeperRegistrar);
    this.zkRegistrationSignal = Optional.ofNullable(builder.zkRegistrationSignal);
    this.retryIntervalPolicy = checkNotNull(builder.retryIntervalPolicy);
    this.sleeper = checkNotNull(builder.sleeper);
    this.reactor = new DefaultReactor("zk-client-async-init", new Update());
  }

  private ConnectionStateListener listener = new ConnectionStateListener() {
    @Override
    public void stateChanged(final CuratorFramework client, final ConnectionState newState) {
      if (newState == ConnectionState.RECONNECTED) {
        reactor.signal();
      }
    }
  };

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private ZooKeeperClient zooKeeperClient;
    private ZooKeeperRegistrar zooKeeperRegistrar;
    private CountDownLatch zkRegistrationSignal;
    private RetryIntervalPolicy retryIntervalPolicy;
    private Sleeper sleeper;

    private Builder() { }

    public Builder setZooKeeperClient(final ZooKeeperClient zooKeeperClient) {
      this.zooKeeperClient = zooKeeperClient;
      return this;
    }

    public Builder setZooKeeperRegistrar(final ZooKeeperRegistrar zooKeeperRegistrar) {
      this.zooKeeperRegistrar = zooKeeperRegistrar;
      return this;
    }

    public Builder setZkRegistrationSignal(final CountDownLatch zkRegistrationSignal) {
      this.zkRegistrationSignal = zkRegistrationSignal;
      return this;
    }

    public Builder setRetryIntervalPolicy(final RetryIntervalPolicy retryIntervalPolicy) {
      this.retryIntervalPolicy = retryIntervalPolicy;
      return this;
    }

    public Builder setSleeper(final Sleeper sleeper) {
      this.sleeper = sleeper;
      return this;
    }

    public ZooKeeperRegistrarService build() {
      if (this.retryIntervalPolicy == null) {
        this.retryIntervalPolicy = BoundedRandomExponentialBackoff.newBuilder()
            .setMinInterval(1, SECONDS)
            .setMaxInterval(30, SECONDS)
            .build();
      }

      if (this.sleeper == null) {
        this.sleeper = new ThreadSleeper();
      }

      return new ZooKeeperRegistrarService(this);
    }
  }

  @Override
  protected void startUp() throws Exception {
    zooKeeperRegistrar.startUp();
    client.getConnectionStateListenable().addListener(listener);
    reactor.startAsync().awaitRunning();
    reactor.signal();
  }

  @Override
  protected void shutDown() throws Exception {
    reactor.stopAsync().awaitTerminated();
    zooKeeperRegistrar.shutDown();
  }

  private class Update implements Reactor.Callback {

    @Override
    public void run(final boolean timeout) throws InterruptedException {
      final RetryScheduler retryScheduler = retryIntervalPolicy.newScheduler();
      while (isAlive()) {
        final long sleep = retryScheduler.nextMillis();

        boolean successfullyRegistered = false;

        try {
          successfullyRegistered = zooKeeperRegistrar.tryToRegister(client);
        } catch (Exception e) {
          if (e instanceof ConnectionLossException) {
            log.warn("ZooKeeper connection lost, retrying registration in {} ms", sleep);
          } else if (e instanceof HostNotFoundException) {
            log.error("ZooKeeper deregistration of old hostname failed, retrying in {} ms: {}",
                sleep, e);
          } else {
            log.error("ZooKeeper registration failed, retrying in {} ms", sleep, e);
          }
        }

        // only exit the loop when zookeeper registration is successful. if registration does not
        // succeed because another host is already registered, this will cause another registration
        // attempt after sleeping. if zookeeper is cannot be connected to, this will also cause
        // another attempt after sleeping.
        if (successfullyRegistered) {
          log.info("Successfully registered host in zookeeper");
          zkRegistrationSignal.ifPresent(CountDownLatch::countDown);
          return;
        } else {
          log.warn("registration not successful, sleeping for {} seconds",
              TimeUnit.MILLISECONDS.toSeconds(sleep));
          sleeper.sleep(sleep);
        }
      }
    }
  }

  private boolean isAlive() {
    return state().ordinal() < STOPPING.ordinal();
  }
}

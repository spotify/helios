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

import static com.google.common.base.Preconditions.checkNotNull;

import com.codahale.metrics.Clock;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.spotify.helios.servicescommon.NoOpRiemannClient;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.statistics.NoopZooKeeperMetrics;
import com.spotify.helios.servicescommon.statistics.ZooKeeperMetrics;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.OperationTimeoutException;
import org.apache.zookeeper.KeeperException.RuntimeInconsistencyException;

public class ZooKeeperModelReporter {
  private final RiemannFacade riemannFacade;
  private final ZooKeeperMetrics metrics;
  private final ImmutableMap<Class<?>, String> exceptionMap =
      ImmutableMap.of(
          OperationTimeoutException.class, "timeout",
          ConnectionLossException.class, "connection_loss",
          RuntimeInconsistencyException.class, "inconsistency");
  private final Clock clock = Clock.defaultClock();

  public ZooKeeperModelReporter(final RiemannFacade riemannFacade,
                                final ZooKeeperMetrics metrics) {
    this.metrics = checkNotNull(metrics);
    this.riemannFacade = checkNotNull(riemannFacade).stack("zookeeper");
  }

  public void checkException(Exception ex, String... tags) {
    Throwable th = ex;
    while (th != null && !(th instanceof KeeperException)) {
      th = th.getCause();
    }
    if (th == null) {
      return;
    }
    final KeeperException k = (KeeperException) th;
    final String message = exceptionMap.get(k.getClass());
    if (message == null) {
      return;
    }
    final List<String> tagList = Lists.newArrayList("zookeeper", "error", message);
    tagList.addAll(Lists.newArrayList(tags));
    riemannFacade.event()
        .tags(tagList)
        .send();
    metrics.zookeeperTransientError();
  }

  public <T> T time(final String tag, final String name, ZooKeeperCallable<T> callable)
      throws KeeperException {
    final long startTime = clock.getTick();
    try {
      return callable.call();
    } catch (KeeperException e) {
      checkException(e, tag, name);
      throw e;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      metrics.updateTimer(name, clock.getTick() - startTime, TimeUnit.NANOSECONDS);
    }
  }

  public void connectionStateChanged(final ConnectionState newState) {
    metrics.connectionStateChanged(newState);
  }

  public static ZooKeeperModelReporter noop() {
    return new ZooKeeperModelReporter(new NoOpRiemannClient().facade(), new NoopZooKeeperMetrics());
  }

  @FunctionalInterface
  public interface ZooKeeperCallable<T> {
    T call() throws KeeperException;
  }
}

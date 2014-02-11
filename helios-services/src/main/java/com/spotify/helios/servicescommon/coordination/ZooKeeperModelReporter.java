package com.spotify.helios.servicescommon.coordination;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.spotify.helios.servicescommon.NoOpRiemannClient;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.statistics.NoopZooKeeperMetrics;
import com.spotify.helios.servicescommon.statistics.ZooKeeperMetrics;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.OperationTimeoutException;
import org.apache.zookeeper.KeeperException.RuntimeInconsistencyException;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ZooKeeperModelReporter {
  private final RiemannFacade riemannFacade;
  private final ZooKeeperMetrics metrics;
  private final ImmutableMap<Class<?>, String> exceptionMap =
      ImmutableMap.<Class<?>, String>of(
          OperationTimeoutException.class, "timeout",
          ConnectionLossException.class, "connection_loss",
          RuntimeInconsistencyException.class, "inconsistency");

  public ZooKeeperModelReporter(final RiemannFacade riemannFacade,
                                final ZooKeeperMetrics metrics) {
    this.metrics = checkNotNull(metrics);
    this.riemannFacade = checkNotNull(riemannFacade);
  }

  public void checkException(Exception e, String... tags) {
    Throwable t = e;
    while (t != null && t.getClass() != KeeperException.class) {
      t = t.getCause();
    }
    if (t == null) {
      return;
    }
    final KeeperException k = (KeeperException) t;
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

  public static ZooKeeperModelReporter noop() {
    return new ZooKeeperModelReporter(NoOpRiemannClient.facade(), new NoopZooKeeperMetrics());
  }
}

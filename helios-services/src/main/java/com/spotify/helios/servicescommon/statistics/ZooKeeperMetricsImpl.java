package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.concurrent.TimeUnit;

public class ZooKeeperMetricsImpl implements ZooKeeperMetrics {
  private static final String TYPE = "zookeeper";

  private final MetricName transientErrorCounterName;
  private final Counter transientErrorCounter;
  private final Meter transientErrorMeter;

  private final MetricName transientErrorMeterName;

  public ZooKeeperMetricsImpl(String group, MetricsRegistry registry) {
    transientErrorCounterName = new MetricName(group, TYPE, "transient_error_count");
    transientErrorMeterName = new MetricName(group, TYPE, "transient_error_meter");

    transientErrorCounter = registry.newCounter(transientErrorCounterName);
    transientErrorMeter = registry.newMeter(transientErrorMeterName, "transientErrors",
        TimeUnit.MINUTES);
  }

  @Override
  public void zookeeperTransientError() {
    transientErrorCounter.inc();
    transientErrorMeter.mark();
  }
}

/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

public class AbstractStatistics<N extends AbstractMetricNames> {

  protected final N names;

  protected final MetricsRegistry registry;

  public AbstractStatistics(final N names, final MetricsRegistry registry) {
    this.names = names;
    this.registry = registry;
  }

  protected Histogram hist(final MetricName name) {
    return hist(name, true);
  }

  private Histogram hist(final MetricName name, final boolean biased) {
    return registry.newHistogram(name, biased);
  }

  protected Meter meter(final MetricName name, final String eventType, final TimeUnit timeUnit) {
    return registry.newMeter(name, eventType, timeUnit);
  }

  protected Meter meter(final MetricName name, final String eventType) {
    return meter(name, eventType, SECONDS);
  }

  protected Counter counter(final MetricName name) {
    return registry.newCounter(name);
  }
}

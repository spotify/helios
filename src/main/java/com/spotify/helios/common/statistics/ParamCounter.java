/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common.statistics;

import com.google.common.collect.Maps;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkNotNull;

public class ParamCounter {

  private final MetricsRegistry registry;
  private final ParamMetricName paramMetricName;
  private final ConcurrentMap<Object, Counter> counters = Maps.newConcurrentMap();

  public ParamCounter(final MetricsRegistry registry, final ParamMetricName paramMetricName) {
    this.registry = checkNotNull(registry);
    this.paramMetricName = checkNotNull(paramMetricName);
  }

  public void inc(final long value, final Object param) {
    Counter counter = counters.get(param);
    if (counter == null) {
      final MetricName name = paramMetricName.name(param);
      counter = registry.newCounter(name);
      final Counter existingCounter = counters.putIfAbsent(param, counter);
      if (existingCounter != null) {
        counter = existingCounter;
      }
    }
    counter.inc(value);
  }
}

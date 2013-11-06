/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.service.statistics;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.AbstractList;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class ParamMeter {

  private final MetricsRegistry registry;
  private final ParamMetricName paramMetricName;
  private final String eventType;
  private final TimeUnit timeUnit;
  private final Meter total;
  private final ConcurrentMap<Object, Meter> meters = Maps.newConcurrentMap();

  public ParamMeter(final MetricsRegistry registry, final ParamMetricName paramMetricName,
                    final String eventType, final TimeUnit timeUnit) {
    this.registry = checkNotNull(registry);
    this.paramMetricName = checkNotNull(paramMetricName);
    this.eventType = checkNotNull(eventType);
    this.timeUnit = checkNotNull(timeUnit);

    this.total = registry.newMeter(paramMetricName.total, eventType, timeUnit);
  }

  public void mark(final Object param) {
    Meter meter = meters.get(param);
    if (meter == null) {
      final MetricName name = paramMetricName.name(param);
      meter = registry.newMeter(name, eventType, timeUnit);
      final Meter existingMeter = meters.putIfAbsent(param, meter);
      if (existingMeter != null) {
        meter = existingMeter;
      }
    }
    meter.mark();
    total.mark();
  }

  public void mark(final Object param1, final Object param2) {
    mark(paramList(param1, param2));
  }

  private Object paramList(final Object param1, final Object param2) {
    return new ParamList2(param1, param2);
  }

  public Meter getTotal() {
    return total;
  }

  private abstract class ParamList extends AbstractList<Object> {

    public String toString() {
      return Joiner.on(" - ").join(this);
    }
  }

  private class ParamList2 extends ParamList {

    private final Object param1;
    private final Object param2;

    public ParamList2(final Object param1, final Object param2) {
      this.param1 = param1;
      this.param2 = param2;
    }

    @Override
    public Object get(final int index) {
      switch (index) {
        case 0:
          return param1;
        case 1:
          return param2;
        default:
          throw new IndexOutOfBoundsException(index + " not in [0, 1]");
      }
    }

    @Override
    public int size() {
      return 2;
    }
  }
}

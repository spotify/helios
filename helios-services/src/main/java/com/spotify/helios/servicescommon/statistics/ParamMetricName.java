/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.servicescommon.statistics;

import com.spotify.statistics.MetricFilter;
import com.yammer.metrics.core.MetricName;

public class ParamMetricName implements MetricFilter {

  private final String group;
  private final String type;
  private final String namePrefix;
  private final String scope;

  public final MetricName total;

  public ParamMetricName(final String group, final String type, final String namePrefix,
                         final String scope) {
    this.group = group;
    this.type = type;
    this.namePrefix = namePrefix;
    this.scope = scope;

    // put "total" in front of the prefix so that this name doesn't match
    this.total = new MetricName(group, type, "total " + namePrefix, scope);
  }

  @Override
  public boolean matches(final MetricName metricName) {
    return metricName.getGroup().equals(group) &&
           metricName.getType().equals(type) &&
           metricName.getName().startsWith(namePrefix) &&
           metricName.hasScope() && metricName.getScope().equals(scope);
  }

  public MetricName name(final Object param) {
    return new MetricName(group, type, namePrefix + " - " + param, scope);
  }
}

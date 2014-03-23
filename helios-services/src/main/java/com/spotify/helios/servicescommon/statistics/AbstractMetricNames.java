/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.MetricName;

public class AbstractMetricNames {

  public final String group;
  public final String type;
  public final String scope;

  public AbstractMetricNames(final String group,
                             final String type,
                             final String scope) {

    this.group = group;
    this.type = type;
    this.scope = scope;
  }

  protected MetricName n(final String name) {
    return new MetricName(group, type, name, scope);
  }
}

/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.servicescommon.statistics;

import com.google.common.collect.Maps;

import com.spotify.helios.servicescommon.MasterRequestMetrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.Map;

public class MasterMetricsImpl implements MasterMetrics {
  private static final String TYPE = "master";

  private final Map<String, MasterRequestMetrics> requestMetrics = Maps.newConcurrentMap();

  private final MetricName jobsInJobList;
  private final MetricName eventsInJobHistory;

  private final Histogram jobsInJobListHist;
  private final Histogram eventsInJobHistoryHist;
  private final String group;
  private final MetricsRegistry registry;

  public MasterMetricsImpl(final String group,
                           final MetricsRegistry registry) {
    this.group = group;
    this.registry = registry;

    eventsInJobHistory = new MetricName(group, TYPE, "events_in_job_history");
    jobsInJobList = new MetricName(group, TYPE, "jobs_in_job_list");

    eventsInJobHistoryHist = registry.newHistogram(eventsInJobHistory, true);
    jobsInJobListHist = registry.newHistogram(jobsInJobList, true);
  }

  @Override
  public void success(final String name) {
    request(name).success();
  }

  @Override
  public void failure(final String name) {
    request(name).failure();
  }

  @Override
  public void badRequest(final String name) {
    request(name).userError();
  }

  private MasterRequestMetrics request(final String name) {
    MasterRequestMetrics m = requestMetrics.get(name);
    if (m == null) {
      m = new MasterRequestMetrics(group, TYPE, name, registry);
      requestMetrics.put(name, m);
    }
    return m;
  }

  @Override
  public void jobsInJobList(final int count) {
    jobsInJobListHist.update(count);
  }

  @Override
  public void jobsHistoryEventSize(final int count) {
    eventsInJobHistoryHist.update(count);
  }
}

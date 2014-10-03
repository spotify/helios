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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

public class MasterMetricsImpl implements MasterMetrics {
  private static final String TYPE = "master";

  private final Map<String, MasterRequestMetrics> requestMetrics = Maps.newConcurrentMap();

  private final Histogram jobsInJobListHist;
  private final Histogram eventsInJobHistoryHist;
  private final String group;
  private final MetricRegistry registry;

  public MasterMetricsImpl(final String group,
                           final MetricRegistry registry) {
    this.group = group;
    this.registry = registry;

    eventsInJobHistoryHist = registry.histogram(name(group, TYPE + "_events_in_job_history"));
    jobsInJobListHist = registry.histogram(name(group, TYPE + "_jobs_in_job_list"));
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

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

package com.spotify.helios.master.metrics;

import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import com.sun.jersey.spi.container.ResourceMethodDispatchAdapter;
import com.sun.jersey.spi.container.ResourceMethodDispatchProvider;

public class ReportingResourceMethodDispatchAdapter implements ResourceMethodDispatchAdapter {

  private final MasterMetrics metrics;

  public ReportingResourceMethodDispatchAdapter(final MasterMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public ResourceMethodDispatchProvider adapt(final ResourceMethodDispatchProvider provider) {
    return new ReportingResourceMethodDispatchProvider(provider, metrics);
  }
}

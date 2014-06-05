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
import com.sun.jersey.api.model.AbstractResourceMethod;
import com.sun.jersey.spi.container.ResourceMethodDispatchProvider;
import com.sun.jersey.spi.dispatch.RequestDispatcher;

public class ReportingResourceMethodDispatchProvider implements ResourceMethodDispatchProvider {

  private final ResourceMethodDispatchProvider provider;
  private final MasterMetrics metrics;

  public ReportingResourceMethodDispatchProvider(final ResourceMethodDispatchProvider provider,
                                                 final MasterMetrics metrics) {
    this.provider = provider;
    this.metrics = metrics;
  }

  @Override
  public RequestDispatcher create(final AbstractResourceMethod abstractResourceMethod) {
    final RequestDispatcher dispatcher = provider.create(abstractResourceMethod);
    return new ReportingResourceMethodDispatcher(dispatcher, metrics);
  }
}

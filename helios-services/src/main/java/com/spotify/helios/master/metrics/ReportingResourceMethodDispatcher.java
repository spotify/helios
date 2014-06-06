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

import com.google.common.collect.Maps;

import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.model.AbstractResourceMethod;
import com.sun.jersey.spi.dispatch.RequestDispatcher;

import java.util.concurrent.ConcurrentMap;

import javax.ws.rs.WebApplicationException;

public class ReportingResourceMethodDispatcher implements RequestDispatcher {

  private final RequestDispatcher dispatcher;
  private final MasterMetrics metrics;
  private final ConcurrentMap<Object, String> keys = Maps.newConcurrentMap();

  public ReportingResourceMethodDispatcher(final RequestDispatcher dispatcher,
                                           final MasterMetrics metrics) {
    this.dispatcher = dispatcher;
    this.metrics = metrics;
  }

  @Override
  public void dispatch(final Object resource, final HttpContext context) {
    final AbstractResourceMethod resourceMethod = context.getUriInfo().getMatchedMethod();
    final String key = getKey(resourceMethod);
    try {
      dispatcher.dispatch(resource, context);
      metrics.success(key);
    } catch (WebApplicationException e) {
      final int status = e.getResponse().getStatus();
      if (status == 404) {
        metrics.success(key);
      } else if (status > 400 && status < 500) {
        metrics.badRequest(key);
      } else {
        metrics.failure(key);
      }
      throw e;
    } catch (Exception e) {
      metrics.failure(key);
      throw e;
    }
  }

  private String getKey(final AbstractResourceMethod resourceMethod) {
    final String key = keys.get(resourceMethod);
    if (key == null) {
      final String name = resourceMethod.getMethod().getDeclaringClass().getSimpleName() + "#" +
                          resourceMethod.getMethod().getName();
      keys.put(resourceMethod, name);
      return name;
    }
    return key;
  }
}

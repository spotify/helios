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

package com.spotify.helios.servicescommon;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import com.spotify.helios.common.HeliosRuntimeException;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.jetty.HttpsConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;

public class ServiceUtil {

  private static final List<String> VALID_PROTOCOLS = ImmutableList.of("http", "https");

  public static DefaultServerFactory createServerFactory(final URI endpoint,
                                                         final URI adminEndpoint,
                                                         final boolean noHttp) {
    final DefaultServerFactory serverFactory = new DefaultServerFactory();
    if (noHttp) {
      serverFactory.setApplicationConnectors(Collections.<ConnectorFactory>emptyList());
      serverFactory.setAdminConnectors(Collections.<ConnectorFactory>emptyList());
    } else {
      final String endpointScheme = endpoint.getScheme();
      final HttpConnectorFactory serviceConnector = getServiceConnector(endpointScheme);
      serviceConnector.setPort(endpoint.getPort());
      serviceConnector.setBindHost(endpoint.getHost());
      serverFactory.setApplicationConnectors(ImmutableList.<ConnectorFactory>of(serviceConnector));

      final String adminEndpointScheme = adminEndpoint.getScheme();
      final HttpConnectorFactory adminConnector = getServiceConnector(adminEndpointScheme);
      adminConnector.setPort(adminEndpoint.getPort());
      serviceConnector.setBindHost(adminEndpoint.getHost());
      serverFactory.setAdminConnectors(ImmutableList.<ConnectorFactory>of(adminConnector));
    }

    return serverFactory;
  }

  private static HttpConnectorFactory getServiceConnector(final String scheme) {
    final HttpConnectorFactory serviceConnector;
    switch (scheme) {
      case "http":
        serviceConnector = new HttpConnectorFactory();
        break;
      case "https":
        serviceConnector = new HttpsConnectorFactory();
        break;
      default:
        throw new HeliosRuntimeException(String.format(
            "Unrecognized server endpoint scheme of '%s'. Must be one of %s.",
            scheme, Joiner.on(", ").join(VALID_PROTOCOLS)));
    }

    return serviceConnector;
  }
}

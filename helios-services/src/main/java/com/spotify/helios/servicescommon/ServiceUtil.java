/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.servicescommon;

import com.google.common.collect.ImmutableList;

import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;

public class ServiceUtil {
  public static DefaultServerFactory createServerFactory(final InetSocketAddress httpEndpoint,
                                                         final InetSocketAddress adminEndpoint,
                                                         final boolean noHttp) {
    final DefaultServerFactory serverFactory = new DefaultServerFactory();
    if (noHttp) {
      serverFactory.setApplicationConnectors(Collections.<ConnectorFactory>emptyList());
      serverFactory.setAdminConnectors(Collections.<ConnectorFactory>emptyList());
    } else {
      final HttpConnectorFactory serviceConnector = new HttpConnectorFactory();
      serviceConnector.setPort(httpEndpoint.getPort());
      serviceConnector.setBindHost(httpEndpoint.getHostString());
      serverFactory.setApplicationConnectors(ImmutableList.<ConnectorFactory>of(serviceConnector));

      final HttpConnectorFactory adminConnector = new HttpConnectorFactory();
      adminConnector.setPort(adminEndpoint.getPort());
      adminConnector.setBindHost(adminEndpoint.getHostString());
      serverFactory.setAdminConnectors(ImmutableList.<ConnectorFactory>of(adminConnector));
    }
    return serverFactory;
  }
}

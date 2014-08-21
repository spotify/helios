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

package com.spotify.helios.agent;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.DockerTimeoutException;
import com.spotify.helios.servicescommon.RiemannFacade;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * A docker client proxy used to monitor docker operations.  It's abstract and doesn't implement
 * {@link DockerClient}, but don't let it fool you.  You call {@link #wrap} with the
 * {@link RiemmanFacade} and the real {@link DockerClient} and you then use that.
 */
public abstract class MonitoredDockerClient {

  private MonitoredDockerClient() {
  }

  public static DockerClient wrap(RiemannFacade riemann, final DockerClient client) {
    return (DockerClient) Proxy.newProxyInstance(
        MonitoredDockerClient.class.getClassLoader(),
        new Class[]{DockerClient.class},
        new MonitoringInvocationHandler(riemann, client));
  }

  private static class MonitoringInvocationHandler implements InvocationHandler {

    private final RiemannFacade riemann;
    private final DockerClient client;

    public MonitoringInvocationHandler(final RiemannFacade riemann, final DockerClient client) {
      this.riemann = riemann;

      this.client = client;
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args)
        throws Throwable {
      try {
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof DockerException) {
          final String tag;
          if (e.getCause() instanceof DockerTimeoutException) {
            tag = "timeout";
          } else {
            tag = "error";
          }
          riemann.event()
              .service("helios-agent/docker")
              .tags("docker", tag, method.getName())
              .send();
        }
        throw e.getCause();
      }
    }
}
}

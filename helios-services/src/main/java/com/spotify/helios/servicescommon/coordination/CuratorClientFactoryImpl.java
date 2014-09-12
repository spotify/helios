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

package com.spotify.helios.servicescommon.coordination;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorClientFactoryImpl implements CuratorClientFactory {
  private static final Logger log = LoggerFactory.getLogger(CuratorClientFactoryImpl.class);

    @Override
    public CuratorFramework newClient(String connectString,
                                      int sessionTimeoutMs,
                                      int connectionTimeoutMs,
                                      RetryPolicy retryPolicy,
                                      String namespace) {
      Builder builder = CuratorFrameworkFactory.builder()
          .connectString(connectString)
          .sessionTimeoutMs(sessionTimeoutMs)
          .connectionTimeoutMs(connectionTimeoutMs)
          .retryPolicy(retryPolicy);

      if (namespace != null) {
        log.info("Setting ZooKeeper namespace to " + namespace);
        builder = builder.namespace(namespace);
      }

      return builder.build();
    }
}

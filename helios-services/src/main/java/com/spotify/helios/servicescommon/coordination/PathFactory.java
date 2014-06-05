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

import org.apache.zookeeper.common.PathUtils;

public class PathFactory {

  private final String base;

  public PathFactory(final String base, final String... parts) {
    final String joined = join(base, parts);
    this.base = joined.startsWith("/") ? joined : "/" + joined;
    PathUtils.validatePath(base);
  }

  public String path(final String... parts) {
    final String path = join(base, parts);
    PathUtils.validatePath(path);
    return path;
  }

  private String join(final String base, final String... parts) {
    final StringBuilder builder = new StringBuilder(base);
    for (final String part : parts) {
      if (part.isEmpty()) {
        continue;
      }
      if (builder.charAt(builder.length() - 1) != '/' && part.charAt(0) != '/') {
        builder.append('/');
      }
      builder.append(part);
    }
    return builder.toString();
  }
}

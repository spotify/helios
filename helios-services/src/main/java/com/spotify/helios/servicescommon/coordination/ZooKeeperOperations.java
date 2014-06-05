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

import com.spotify.helios.common.descriptors.Descriptor;

import java.util.List;
import java.util.Map;

public class ZooKeeperOperations {
  public static ZooKeeperOperation set(final String path, Descriptor data) {
    return new SetData(path, data.toJsonBytes());
  }

  public static ZooKeeperOperation set(final String path, byte[] bytes) {
    return new SetData(path, bytes);
  }

  public static ZooKeeperOperation check(final String path, final int version) {
    return new CheckWithVersion(path, version);
  }

  public static ZooKeeperOperation check(final String path) {
    return new Check(path);
  }

  public static ZooKeeperOperation check(final Node node) {
    return new CheckWithVersion(node.getPath(), node.getStat().getVersion());
  }

  public static ZooKeeperOperation create(final Map<String, byte[]> nodes) {
    return new CreateMany(nodes);
  }

  public static ZooKeeperOperation create(final String path) {
    return new CreateEmpty(path);
  }

  public static ZooKeeperOperation create(final String path, final Descriptor data) {
    return create(path, data.toJsonBytes());
  }

  public static ZooKeeperOperation create(final String path, final Descriptor data,
                                          final int version) {
    return create(path, data.toJsonBytes(), version);
  }

  public static ZooKeeperOperation create(final String path, final byte[] bytes,
                                          final int version) {
    return new CreateWithDataAndVersion(path, bytes, version);
  }

  public static ZooKeeperOperation create(final String path, final byte[] bytes) {
    return new CreateWithData(path, bytes);
  }

  public static ZooKeeperOperation delete(final String path) {
    return new Delete(path);
  }

  public static ZooKeeperOperation delete(final List<String> paths) {
    return new DeleteMany(paths);
  }
}


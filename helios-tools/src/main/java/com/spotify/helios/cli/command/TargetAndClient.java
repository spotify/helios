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

package com.spotify.helios.cli.command;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import com.spotify.helios.cli.Target;
import com.spotify.helios.client.HeliosClient;

public class TargetAndClient {
  private final Optional<Target> target;
  private final HeliosClient client;

  public TargetAndClient(final Target target, final HeliosClient client) {
    this.target = Optional.of(Preconditions.checkNotNull(target));
    this.client = client;
  }

  public TargetAndClient(final HeliosClient client) {
    this.target = Optional.absent();
    this.client = client;
  }

  public Optional<Target> getTarget() {
    return target;
  }

  public HeliosClient getClient() {
    return client;
  }
}

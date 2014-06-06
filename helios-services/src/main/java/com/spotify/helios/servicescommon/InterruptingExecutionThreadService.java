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

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class InterruptingExecutionThreadService extends AbstractExecutionThreadService {

  private final ExecutorService executorService;
  private final String name;

  protected InterruptingExecutionThreadService(final String name) {
    this.name = name;
    this.executorService = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setNameFormat(name + "-%d").build());
  }

  @Override
  protected String serviceName() {
    return name;
  }

  @Override
  protected Executor executor() {
    return executorService;
  }

  @Override
  protected void triggerShutdown() {
    executorService.shutdownNow();
  }
}

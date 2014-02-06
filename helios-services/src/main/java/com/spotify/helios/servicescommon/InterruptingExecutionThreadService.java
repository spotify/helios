/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.servicescommon;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class InterruptingExecutionThreadService extends AbstractExecutionThreadService {

  private final ExecutorService executorService = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat(serviceName() + "-%d").build());

  @Override
  protected Executor executor() {
    return executorService;
  }

  @Override
  protected void triggerShutdown() {
    executorService.shutdownNow();
  }
}

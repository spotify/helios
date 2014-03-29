package com.spotify.helios.agent;

import com.spotify.helios.common.Json;
import com.spotify.helios.servicescommon.coordination.NodeUpdaterFactory;
import com.spotify.helios.servicescommon.coordination.ZooKeeperNodeUpdater;
import com.spotify.helios.servicescommon.coordination.Paths;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EnvironmentVariableReporter extends InterruptingScheduledService {

  private static final int RETRY_INTERVAL_MILLIS = 1000;

  final String host;
  private final Map<String, String> envVars;
  private final ZooKeeperNodeUpdater nodeUpdater;

  public EnvironmentVariableReporter(final String host, final Map<String, String> envVars,
                                     final NodeUpdaterFactory nodeUpdaterFactory) {
    this.envVars = envVars;
    this.host = host;
    this.nodeUpdater = nodeUpdaterFactory.create(Paths.statusHostEnvVars(host));
  }


  @Override
  protected void runOneIteration() {
    final boolean succesful = nodeUpdater.update(Json.asBytesUnchecked(envVars));
    if (succesful) {
      stopAsync();
    }
  }

  @Override
  protected ScheduledFuture<?> schedule(Runnable runnable, ScheduledExecutorService executorService) {
    return executorService.scheduleWithFixedDelay(runnable, 0, RETRY_INTERVAL_MILLIS, MILLISECONDS);
  }
}

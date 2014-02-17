package com.spotify.helios.agent;

import com.google.common.util.concurrent.AbstractScheduledService;

import com.spotify.helios.common.Json;
import com.spotify.helios.servicescommon.NodeUpdaterFactory;
import com.spotify.helios.servicescommon.ZooKeeperNodeUpdater;
import com.spotify.helios.servicescommon.coordination.Paths;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EnvironmentVariableReporter extends AbstractScheduledService {

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
  protected void runOneIteration() throws Exception {
    final boolean succesful = nodeUpdater.update(Json.asBytes(envVars));
    if (succesful) {
      stopAsync();
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedDelaySchedule(0, RETRY_INTERVAL_MILLIS, MILLISECONDS);
  }
}

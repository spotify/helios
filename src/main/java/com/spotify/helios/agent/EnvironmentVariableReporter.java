package com.spotify.helios.agent;

import com.google.common.util.concurrent.AbstractScheduledService;

import com.spotify.helios.common.Json;
import com.spotify.helios.common.NodeUpdaterFactory;
import com.spotify.helios.common.ZooKeeperNodeUpdater;
import com.spotify.helios.common.coordination.Paths;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class EnvironmentVariableReporter extends AbstractScheduledService {

  private static final int RETRY_INTERVAL_MILLIS = 1000;

  final String agent;
  private final Map<String, String> envVars;
  private final ZooKeeperNodeUpdater nodeUpdater;

  public EnvironmentVariableReporter(final String agent, final Map<String, String> envVars,
                                     final NodeUpdaterFactory nodeUpdaterFactory) {
    this.envVars = envVars;
    this.agent = agent;
    this.nodeUpdater = nodeUpdaterFactory.create(Paths.statusAgentEnvVars(agent));
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

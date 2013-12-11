package com.spotify.helios.agent;

import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.helios.common.DefaultZooKeeperClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.coordination.Paths;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static com.google.common.util.concurrent.MoreExecutors.getExitingScheduledExecutorService;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;

public class EnvironmentVariableReporter {
  private static final Logger log = LoggerFactory.getLogger(EnvironmentVariableReporter.class);

  public static final int DEFAULT_INTERVAL = 1;
  public static final TimeUnit DEFAUL_TIMEUNIT = MINUTES;

  private final String agent;
  private final DefaultZooKeeperClient zooKeeperClient;
  private final Map<String, String> envVars;
  private final ListeningScheduledExecutorService executor =
      listeningDecorator(getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1)));
  private ListenableScheduledFuture<?> updateFuture;

  public EnvironmentVariableReporter(String agent, Map<String, String> envVars,
                                     DefaultZooKeeperClient zooKeeperClient) {
    this.envVars = envVars;
    this.agent = agent;
    this.zooKeeperClient = zooKeeperClient;
  }

  public void start() {
    updateFuture = executor.scheduleWithFixedDelay(new Report(), 0, DEFAULT_INTERVAL,
        DEFAUL_TIMEUNIT);
  }

  private class Report implements Runnable {
    @Override public void run() {
      while (true) {
        final String path = Paths.statusAgentEnvVars(agent);
        try {
          // Check if the node already exists.
          final Stat stat = zooKeeperClient.stat(path);

          byte[] jsonBytes = Json.asBytes(envVars);
          if (stat != null) {
            // The node already exists, overwrite it.
            zooKeeperClient.setData(path, jsonBytes);
          } else {
            zooKeeperClient.createAndSetData(path, jsonBytes);
          }
          updateFuture.cancel(true);
        } catch (KeeperException | JsonProcessingException e) {
          log.error("Error updating with our environment variables", e);
        }
      }
    }
  }
}

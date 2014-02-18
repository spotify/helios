package com.spotify.helios.agent;

import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.core.HealthCheck;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DockerHealthChecker extends HealthCheck implements Managed {
  private final SupervisorMetrics metrics;
  private final ScheduledExecutorService scheduler;
  private final TimeUnit timeUnit;
  private final int interval;
  private final HealthCheckRunnable runnable;

  public DockerHealthChecker(SupervisorMetrics metrics, TimeUnit timeUnit, int interval) {
    super("docker");
    this.metrics = metrics;
    this.scheduler = Executors.newScheduledThreadPool(1);
    this.timeUnit = timeUnit;
    this.interval = interval;
    this.runnable = new HealthCheckRunnable();
  }

  private class HealthCheckRunnable implements Runnable {
    private boolean dockerIsSick = false;

    @Override
    public void run() {
      // Yay hysteresis!
      if (metrics.getDockerTimeoutRates().getFiveMinuteRate() > 0.8) {
        if (!dockerIsSick) {
          dockerIsSick = true;
        }
      } else if (metrics.getDockerTimeoutRates().getFiveMinuteRate() < 0.4) {
        if (dockerIsSick) {
          dockerIsSick = false;
        }
      }
    }
  }

  @Override
  public void stop() {
    scheduler.shutdownNow();
  }

  @Override
  public void start() {
    scheduler.scheduleAtFixedRate(runnable, interval, interval, timeUnit);
  }

  @Override
  protected Result check() throws Exception {
    if (runnable.dockerIsSick) {
      return Result.unhealthy("docker timeouts are too high for too long");
    } else {
      return Result.healthy();
    }
  }
}

package com.spotify.helios.agent;

import com.spotify.helios.servicescommon.RiemannFacade;
import com.yammer.dropwizard.lifecycle.Managed;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.HealthCheck;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class NamelessHealthChecker extends HealthCheck implements Managed, Runnable {

  private static final Logger log = LoggerFactory.getLogger(NamelessHealthChecker.class);

  // TODO: these values should be adjusted once we see what actually happens in production
  static final double FAILURE_LOW_WATERMARK = 0.1;
  static final double FAILURE_HIGH_WATERMARK = 0.2;

  private static final MetricName HEARTBEAT_TIMER =
      new MetricName("com.spotify.nameless.client",
                     "HeartbeatNamelessRegistrar",
                     "heartbeats");

  private static final MetricName HEARTBEAT_ERROR_METER =
      new MetricName("com.spotify.nameless.client",
                     "HeartbeatNamelessRegistrar",
                     "heartbeat errors");

  private final TimeUnit timeUnit;
  private final int interval;
  private final RiemannFacade facade;
  private final ScheduledExecutorService scheduler;

  private boolean previousStateOk = true;

  public NamelessHealthChecker(TimeUnit timeUnit, int interval, RiemannFacade facade) {
    super("nameless");
    this.timeUnit = timeUnit;
    this.interval = interval;
    this.facade = facade;
    this.scheduler = Executors.newScheduledThreadPool(1);
  }

  @Override
  protected Result check() throws Exception {
    return check(getHeartbeatErrorMeter(), getHeartbeatTimer());
  }

  // this override method exists for so we can inject dummy metrics during testing
  protected Result check(Metered numerator, Metered denominator) throws Exception {

    final double failureRatio = getFailureRatio(numerator, denominator);

    final boolean currentStateOk;
    if (failureRatio < FAILURE_LOW_WATERMARK) {
      currentStateOk = true;
    } else if (failureRatio > FAILURE_HIGH_WATERMARK) {
      currentStateOk = false;
    } else {
      currentStateOk = previousStateOk;
    }

    if (currentStateOk && !previousStateOk) {
      facade.event()
          .state("ok")
          .tags("nameless", "health")
          .metric(1)
          .send();
    } else if (!currentStateOk && previousStateOk) {
      facade.event()
          .state("critical")
          .tags("nameless", "health")
          .metric(0)
          .send();
    }

    previousStateOk = currentStateOk;

    return currentStateOk ? Result.healthy() :
           Result.unhealthy("nameless failure rate ", failureRatio);
  }

  @Override
  public void start() throws Exception {
    scheduler.scheduleAtFixedRate(this, interval, interval, timeUnit);
  }

  @Override
  public void stop() throws Exception {
    scheduler.shutdownNow();
  }

  @Override
  public void run() {
    try {
      check();
    } catch (Exception e) {
      log.error("Exception while running nameless health check", e);
    }
  }

  private static double getFailureRatio(Metered numerator, Metered denominator) {
    if (numerator == null || numerator.fiveMinuteRate() == 0 ||
        denominator == null || denominator.fiveMinuteRate() == 0) {
      return 0;
    }

    return numerator.fiveMinuteRate() / denominator.fiveMinuteRate();
  }

  protected static Meter getHeartbeatErrorMeter() {
    return (Meter) Metrics.defaultRegistry().allMetrics().get(HEARTBEAT_ERROR_METER);
  }

  protected static Timer getHeartbeatTimer() {
    return (Timer) Metrics.defaultRegistry().allMetrics().get(HEARTBEAT_TIMER);
  }

}

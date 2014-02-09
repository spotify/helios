package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.concurrent.TimeUnit;

public class RequestMetrics {

  private final Counter successCounter;
  private final Counter failureCounter;
  private final Counter userErrorCounter;
  private final Timer timer;
  private final MetricName successName;
  private final MetricName failureName;
  private final MetricName userErrorName;
  private final MetricName timerName;

  public RequestMetrics(String group, String type, String requestName) {

    successName = new MetricName(group, type, requestName + "_successful");
    failureName = new MetricName(group, type, requestName + "_failed");
    userErrorName = new MetricName(group, type, requestName + "_failed");
    timerName = new MetricName(group, type, requestName + "_latency");

    final MetricsRegistry registry = MetricsImpl.getRegistry();
    successCounter = registry.newCounter(successName);
    failureCounter = registry.newCounter(failureName);
    userErrorCounter = registry.newCounter(userErrorName);
    timer = registry.newTimer(timerName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
  }

  public TimerContext begin() {
    return timer.time();
  }

  public void success(TimerContext context) {
    success();
    context.stop();
  }

  public void success() {
    successCounter.inc();
  }

  public void failure(TimerContext context) {
    failure();
    context.stop();
  }

  public void failure() {
    failureCounter.inc();
  }

  public void userError(TimerContext context) {
    userError();
    context.stop();
  }

  public void userError() {
    userErrorCounter.inc();
  }

  public Counter getSuccessCounter() {
    return successCounter;
  }

  public Counter getFailureCounter() {
    return failureCounter;
  }

  public Counter getUserErrorCounter() {
    return failureCounter;
  }

  public Timer getTimer() {
    return timer;
  }

  public MetricName getSuccessName() {
    return successName;
  }

  public MetricName getFailureName() {
    return failureName;
  }

  public MetricName getTimerName() {
    return timerName;
  }

  public MetricName getUserErrorName() {
    return userErrorName;
  }
}

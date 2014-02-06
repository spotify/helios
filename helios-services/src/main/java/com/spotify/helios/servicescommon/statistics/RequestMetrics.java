package com.spotify.helios.servicescommon.statistics;

import com.spotify.statistics.MuninGraphCategoryConfig;
import com.spotify.statistics.Property;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.util.concurrent.TimeUnit;

import static com.spotify.helios.servicescommon.statistics.MetricsImpl.getGraphName;
import static com.spotify.helios.servicescommon.statistics.MetricsImpl.getMuninName;

public class RequestMetrics {

  private final Counter successCounter;
  private final Counter failureCounter;
  private final Counter userErrorCounter;
  private final Timer timer;
  private final MetricName successName;
  private final MetricName failureName;
  private final MetricName userErrorName;
  private final MetricName timerName;

  public RequestMetrics(MuninGraphCategoryConfig category, String group,
                        String type, String requestName) {

    successName = new MetricName(group, type, requestName + "_successful");
    failureName = new MetricName(group, type, requestName + "_failed");
    userErrorName = new MetricName(group, type, requestName + "_failed");
    timerName = new MetricName(group, type, requestName + "_latency");

    final MetricsRegistry registry = MetricsImpl.getRegistry();
    successCounter = registry.newCounter(successName);
    failureCounter = registry.newCounter(failureName);
    userErrorCounter = registry.newCounter(userErrorName);
    timer = registry.newTimer(timerName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    category.graph(getGraphName(requestName, "requests"))
        .muninName(getMuninName(group, type, requestName, "requests"))
        .vlabel("Requests")
        .dataSource(successName, "Success")
        .dataSource(failureName, "Failure")
        .dataSource(userErrorName, "UserError");

    category.graph(getGraphName(requestName, "latency (time)"))
        .muninName(getMuninName(group, type, requestName, "time"))
        .vlabel("Milliseconds")
        .dataSource(timerName, "Median", Property.TimerProperty.MEDIAN)
        .dataSource(timerName, "95%", Property.TimerProperty.PERCENTILE95)
        .dataSource(timerName, "99%", Property.TimerProperty.PERCENTILE99)
        .dataSource(timerName, "99.9%", Property.TimerProperty.PERCENTILE999);

    category.graph(getGraphName(requestName, "latency (rate)"))
        .muninName(getMuninName(group, type, requestName, "rate"))
        .vlabel("Requests")
        .dataSource(timerName, "1 minute rate", Property.TimerProperty.ONE_MINUTE_RATE)
        .dataSource(timerName, "5 minute rate", Property.TimerProperty.FIVE_MINUTE_RATE)
        .dataSource(timerName, "15 minute rate", Property.TimerProperty.FIFTEEN_MINUTE_RATE);
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

package com.spotify.helios.servicescommon;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

public class MasterRequestMetrics {

  private final Counter successCounter;
  private final Counter failureCounter;
  private final Counter userErrorCounter;

  private final MetricName successName;
  private final MetricName failureName;
  private final MetricName userErrorName;

  public MasterRequestMetrics(String group, String type, String requestName,
                              final MetricsRegistry registry) {

    successName = new MetricName(group, type, requestName + "_success");
    failureName = new MetricName(group, type, requestName + "_failures");
    userErrorName = new MetricName(group, type, requestName + "_usererror");

    successCounter = registry.newCounter(successName);
    failureCounter = registry.newCounter(failureName);
    userErrorCounter = registry.newCounter(userErrorName);
  }

  public void success() {
    successCounter.inc();
  }

  public void failure() {
    failureCounter.inc();
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

  public MetricName getSuccessName() {
    return successName;
  }

  public MetricName getFailureName() {
    return failureName;
  }

  public MetricName getUserErrorName() {
    return userErrorName;
  }
}

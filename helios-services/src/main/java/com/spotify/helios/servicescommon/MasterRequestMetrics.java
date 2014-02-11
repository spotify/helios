package com.spotify.helios.servicescommon;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.concurrent.TimeUnit;

public class MasterRequestMetrics {

  private final Counter successCounter;
  private final Counter failureCounter;
  private final Counter userErrorCounter;

  private final MetricName successMeterName;
  private final MetricName failureMeterName;
  private final MetricName userErrorMeterName;
  private final MetricName userErrorCounterName;
  private final MetricName failureCounterName;
  private final MetricName successCounterName;
  private final Meter successMeter;
  private final Meter failureMeter;
  private final Meter userErrorMeter;

  public MasterRequestMetrics(String group, String type, String requestName,
                              final MetricsRegistry registry) {
    successCounterName = new MetricName(group, type, requestName + "_count_success");
    failureCounterName = new MetricName(group, type, requestName + "_count_failures");
    userErrorCounterName = new MetricName(group, type, requestName + "_count_usererror");

    successMeterName = new MetricName(group, type, requestName + "_meter_success");
    failureMeterName = new MetricName(group, type, requestName + "_meter_failures");
    userErrorMeterName = new MetricName(group, type, requestName + "_meter_usererror");

    successCounter = registry.newCounter(successCounterName);
    failureCounter = registry.newCounter(failureCounterName);
    userErrorCounter = registry.newCounter(userErrorCounterName);

    successMeter = registry.newMeter(successMeterName, "successes", TimeUnit.SECONDS);
    failureMeter = registry.newMeter(failureMeterName, "failures", TimeUnit.SECONDS);
    userErrorMeter = registry.newMeter(userErrorMeterName, "user_errors", TimeUnit.SECONDS);
  }

  public void success() {
    successCounter.inc();
    successMeter.mark();
  }

  public void failure() {
    failureCounter.inc();
    failureMeter.mark();
  }

  public void userError() {
    userErrorCounter.inc();
    userErrorMeter.mark();
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
    return successMeterName;
  }

  public MetricName getFailureName() {
    return failureMeterName;
  }

  public MetricName getUserErrorName() {
    return userErrorMeterName;
  }
}

package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.TimerContext;

/**
 * Creation of this class marks the beginning of a call to some service. Either {@link #success()}
 * {@link #userError()} or {@link #failure()} should be called when the call completes to record the
 * duration of the call and increment the call counter.
 */
public class MetricsContextImpl implements MetricsContext {

  private final RequestMetrics metrics;
  private final TimerContext timerContext;

  public MetricsContextImpl(RequestMetrics metrics) {
    this.metrics = metrics;
    this.timerContext = metrics.begin();
  }

  @Override
  public void success() {
    metrics.success(timerContext);
  }

  @Override
  public void failure() {
    metrics.failure(timerContext);
  }

  @Override
  public void userError() {
    metrics.userError(timerContext);
  }
}

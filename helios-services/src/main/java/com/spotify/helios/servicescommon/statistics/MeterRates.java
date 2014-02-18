package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.Meter;

public class MeterRates {
  private final double oneMinuteRate;
  private final double fiveMinuteRate;
  private final double fifteenMinuteRate;

  public MeterRates(Meter meter) {
    this(meter.oneMinuteRate(), meter.fiveMinuteRate(), meter.fifteenMinuteRate());
  }

  public MeterRates(double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate) {
    this.oneMinuteRate = oneMinuteRate;
    this.fiveMinuteRate = fiveMinuteRate;
    this.fifteenMinuteRate = fifteenMinuteRate;
  }

  public double getOneMinuteRate() {
    return oneMinuteRate;
  }

  public double getFiveMinuteRate() {
    return fiveMinuteRate;
  }

  public double getFifteenMinuteRate() {
    return fifteenMinuteRate;
  }
}

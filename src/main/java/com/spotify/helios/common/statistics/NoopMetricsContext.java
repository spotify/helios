package com.spotify.helios.common.statistics;

public class NoopMetricsContext implements MetricsContext {

  @Override
  public void success() {}

  @Override
  public void failure() {}

  @Override
  public void userError() {}

}

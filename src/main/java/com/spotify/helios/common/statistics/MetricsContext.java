package com.spotify.helios.common.statistics;

public interface MetricsContext {

  public void success();

  public void failure();

  public void userError();

}

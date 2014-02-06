package com.spotify.helios.servicescommon.statistics;

public interface MetricsContext {

  public void success();

  public void failure();

  public void userError();

}

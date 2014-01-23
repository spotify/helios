package com.spotify.helios.common.statistics;

public class NoopMasterMetrics implements MasterMetrics {

  @Override
  public MetricsContext beginRequest(RequestType requestType) {
    return new NoopMetricsContext();
  }

  @Override
  public void jobsInJobList(int count) {}

  @Override
  public void jobsHistoryEventSize(int count) {}

}

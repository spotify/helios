package com.spotify.helios.servicescommon.statistics;

public class NoopMasterMetrics implements MasterMetrics {

  @Override
  public MetricsContext beginRequest(RequestType requestType) {
    return new NoopMetricsContext();
  }

  @Override
  public void jobsInJobList(int count) {}

  @Override
  public void jobsHistoryEventSize(int count) {}

  @Override
  public void zookeeperTransientError() {}
}

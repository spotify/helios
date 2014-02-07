package com.spotify.helios.servicescommon.statistics;

public interface MasterMetrics {

  /**
   * This method should be called each time a request comes into the {@link MasterHandler}.
   * When the call completes, the returned instance of {@link MetricsContextImpl} should
   * be invoked to indicate if the call was successful or not.
   *
   * @return An instance of {@link MetricsContextImpl} which should be invoked when the call completes
   * to indicate success, user error or failure.
   */
  public MetricsContext beginRequest(RequestType requestType);

  public void jobsInJobList(int count);

  public void jobsHistoryEventSize(int count);

  void zookeeperTransientError();
}

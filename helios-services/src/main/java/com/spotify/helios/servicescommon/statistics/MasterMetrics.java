package com.spotify.helios.servicescommon.statistics;

public interface MasterMetrics {

  void success(String name);

  void failure(String name);

  void badRequest(String name);

  void jobsInJobList(int count);

  void jobsHistoryEventSize(int count);
}

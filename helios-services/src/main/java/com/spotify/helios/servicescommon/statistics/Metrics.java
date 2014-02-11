package com.spotify.helios.servicescommon.statistics;

public interface Metrics {

  void start();

  void stop();

  MasterMetrics getMasterMetrics();

  SupervisorMetrics getSupervisorMetrics();

  ZooKeeperMetrics getZooKeeperMetrics();

}

package com.spotify.helios.servicescommon.statistics;

public class NoopZooKeeperMetrics implements ZooKeeperMetrics {
  @Override
  public void zookeeperTransientError() {}
}

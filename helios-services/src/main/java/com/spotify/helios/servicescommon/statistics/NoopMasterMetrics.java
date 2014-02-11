package com.spotify.helios.servicescommon.statistics;

public class NoopMasterMetrics implements MasterMetrics {

  @Override
  public void success(final String name) {
  }

  @Override
  public void failure(final String name) {
  }

  @Override
  public void badRequest(final String name) {
  }

  @Override
  public void jobsInJobList(int count) {}

  @Override
  public void jobsHistoryEventSize(int count) {}
}
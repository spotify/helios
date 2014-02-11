package com.spotify.helios.servicescommon.coordination;

public class ZooKeeperClientProvider {
  private final ZooKeeperClient client;
  private final ZooKeeperModelReporter reporter;

  public ZooKeeperClientProvider(ZooKeeperClient client, ZooKeeperModelReporter reporter) {
    this.client = client;
    this.reporter = reporter;
  }

  public ZooKeeperClient get(String tag) {
    return new ReportingZooKeeperClient(client, reporter, tag);
  }
}

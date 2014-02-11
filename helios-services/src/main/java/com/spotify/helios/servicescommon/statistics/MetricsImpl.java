package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

public class MetricsImpl implements Metrics {

  private static final String GROUP = "helios";

  private final SupervisorMetrics supervisorMetrics;
  private final MasterMetrics masterMetrics;
  private final ZooKeeperMetrics zooKeeperMetrics;
  private final JmxReporter jmxReporter;

  public MetricsImpl(final MetricsRegistry registry) {
    this.masterMetrics = new MasterMetricsImpl(GROUP, registry);
    this.supervisorMetrics = new SupervisorMetricsImpl(GROUP, registry);
    this.zooKeeperMetrics = new ZooKeeperMetricsImpl(GROUP, registry);
    this.jmxReporter = new JmxReporter(registry);
  }

  @Override
  public void start() {
    jmxReporter.start();
  }

  @Override
  public void stop() {
    jmxReporter.shutdown();
  }

  @Override
  public MasterMetrics getMasterMetrics() {
    return masterMetrics;
  }

  @Override
  public SupervisorMetrics getSupervisorMetrics() {
    return supervisorMetrics;
  }

  @Override
  public ZooKeeperMetrics getZooKeeperMetrics() {
    return zooKeeperMetrics;
  }
}

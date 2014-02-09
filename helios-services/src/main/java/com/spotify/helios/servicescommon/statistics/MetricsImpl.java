package com.spotify.helios.servicescommon.statistics;

import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsImpl implements Metrics {

  private static final Logger log = LoggerFactory.getLogger(MetricsImpl.class);
  private static final String GROUP = "helios";
  private MasterMetrics masterMetrics;
  private SupervisorMetrics supervisorMetrics;

  @Override
  public void start() {
    final MetricsRegistry registry = getRegistry();

    // setup JMX reporting
    JmxReporter.startDefault(registry);

    // agent, master and zookeeper metrics
    masterMetrics = new MasterMetricsImpl(GROUP, registry);
    supervisorMetrics = new SupervisorMetricsImpl(GROUP, registry);
  }

  @Override
  public void stop() {
  }

  public static MetricsRegistry getRegistry() {
    return com.yammer.metrics.Metrics.defaultRegistry();
  }

  @Override
  public MasterMetrics getMasterMetrics() {
    return masterMetrics;
  }

  @Override
  public SupervisorMetrics getSupervisorMetrics() {
    return supervisorMetrics;
  }
}

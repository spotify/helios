package com.spotify.helios.servicescommon.statistics;

import com.google.common.collect.Maps;

import com.spotify.helios.servicescommon.MasterRequestMetrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.Map;

public class MasterMetricsImpl implements MasterMetrics {
  private static final String TYPE = "master";

  private final Map<String, MasterRequestMetrics> requestMetrics = Maps.newConcurrentMap();

  private final MetricName jobsInJobList;
  private final MetricName eventsInJobHistory;
  private final MetricName zookeeperTransientError;

  private final Histogram jobsInJobListHist;
  private final Histogram eventsInJobHistoryHist;
  private final Counter zookeeperTransientErrorCounter;
  private final String group;
  private final MetricsRegistry registry;

  public MasterMetricsImpl(final String group,
                           final MetricsRegistry registry) {
    this.group = group;
    this.registry = registry;

    eventsInJobHistory = new MetricName(group, TYPE, "events_in_job_history");
    jobsInJobList = new MetricName(group, TYPE, "jobs_in_job_list");
    zookeeperTransientError = new MetricName(group, TYPE, "zookeeper_transient_error");

    eventsInJobHistoryHist = registry.newHistogram(eventsInJobHistory, true);
    jobsInJobListHist = registry.newHistogram(jobsInJobList, true);
    zookeeperTransientErrorCounter = registry.newCounter(zookeeperTransientError);
  }

  @Override
  public void success(final String name) {
    request(name).success();
  }

  @Override
  public void failure(final String name) {
    request(name).failure();
  }

  @Override
  public void badRequest(final String name) {
    request(name).userError();
  }

  private MasterRequestMetrics request(final String name) {
    MasterRequestMetrics m = requestMetrics.get(name);
    if (m == null) {
      m = new MasterRequestMetrics(group, TYPE, name, registry);
      requestMetrics.put(name, m);
    }
    return m;
  }

  @Override
  public void jobsInJobList(final int count) {
    jobsInJobListHist.update(count);
  }

  @Override
  public void jobsHistoryEventSize(final int count) {
    eventsInJobHistoryHist.update(count);
  }

  @Override
  public void zookeeperTransientError() {
    zookeeperTransientErrorCounter.inc();
  }
}

package com.spotify.helios.servicescommon.statistics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.Map;

/**
 * Defines the metrics which will be collected, and the graphs which will be generated,
 * for all calls to the Affinity service. Exposes methods for updating metrics which
 * should be invoked for each call to the service.
 */
public class MasterMetricsImpl implements MasterMetrics {
  private static final String TYPE = "master";

  private final Map<RequestType, RequestMetrics> metrics;

  private final MetricName jobsInJobList;
  private final MetricName eventsInJobHistory;
  private final MetricName zookeeperTransientError;

  private final Histogram jobsInJobListHist;
  private final Histogram eventsInJobHistoryHist;
  private final Counter zookeeperTransientErrorCounter;

  public MasterMetricsImpl(final String group,
                           final MetricsRegistry registry) {

    final Builder<RequestType, RequestMetrics> metricsBuilder = ImmutableMap.builder();
    final Object[] possibleValues = RequestType.class.getEnumConstants();
    for (int i = 0; i < possibleValues.length; i++) {
      final RequestType t = (RequestType) possibleValues[i];
      final RequestMetrics metric = new RequestMetrics(group, TYPE, t.getMetricsName());
      metricsBuilder.put(t, metric);
    }
    metrics = metricsBuilder.build();

    eventsInJobHistory = new MetricName(group, TYPE, "events_in_job_history");
    jobsInJobList = new MetricName(group, TYPE, "jobs_in_job_list");
    zookeeperTransientError = new MetricName(group, TYPE, "zookeeper_transient_error");

    eventsInJobHistoryHist = registry.newHistogram(eventsInJobHistory, true);
    jobsInJobListHist = registry.newHistogram(jobsInJobList, true);
    zookeeperTransientErrorCounter = registry.newCounter(zookeeperTransientError);
  }

  @Override
  public MetricsContext beginRequest(RequestType requestType) {
    return new MetricsContextImpl(metrics.get(requestType));
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

package com.spotify.helios.servicescommon.statistics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import com.spotify.statistics.MuninGraph;
import com.spotify.statistics.MuninGraphCategoryConfig;
import com.spotify.statistics.MuninReporterConfig;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import java.util.Map;
import java.util.Map.Entry;

import static com.spotify.helios.servicescommon.statistics.MetricsImpl.getGraphName;
import static com.spotify.helios.servicescommon.statistics.MetricsImpl.getMuninName;

/**
 * Defines the metrics which will be collected, and the graphs which will be generated,
 * for all calls to the Affinity service. Exposes methods for updating metrics which
 * should be invoked for each call to the service.
 */
public class MasterMetricsImpl implements MasterMetrics {
  private static final String TYPE = "Master";

  private final Map<RequestType, RequestMetrics> metrics;

  private final MetricName jobsInJobList;
  private final MetricName eventsInJobHistory;

  private final Histogram jobsInJobListHist;
  private final Histogram eventsInJobHistoryHist;


  public MasterMetricsImpl(final String group,
                           final MuninReporterConfig reporterConfig,
                           final MetricsRegistry registry) {
    final MuninGraphCategoryConfig category = reporterConfig.category("master");

    final Builder<RequestType, RequestMetrics> metricsBuilder = ImmutableMap.builder();
    final Object[] possibleValues = RequestType.class.getEnumConstants();
    for (int i = 0; i < possibleValues.length; i++) {
      final RequestType t = (RequestType) possibleValues[i];
      final RequestMetrics metric = new RequestMetrics(category, group, TYPE, t.getMetricsName());
      metricsBuilder.put(t, metric);
    }
    metrics = metricsBuilder.build();

    eventsInJobHistory = new MetricName(group, TYPE, "events_in_job_history");
    jobsInJobList = new MetricName(group, TYPE, "jobs_in_job_list");

    eventsInJobHistoryHist = registry.newHistogram(eventsInJobHistory, true);
    jobsInJobListHist = registry.newHistogram(jobsInJobList, true);

    final String name = "total_requests";
    MuninGraph.Builder builder = category.graph(getGraphName(name))
        .muninName(getMuninName(group, TYPE, name))
        .vlabel("Requests");

    for (Entry<RequestType, RequestMetrics> entry : metrics.entrySet()) {
      final RequestType type = entry.getKey();
      final RequestMetrics metric = entry.getValue();
      builder = builder.dataSource(metric.getFailureName(), type.getHumanName() + " Failure");
      builder = builder.dataSource(metric.getSuccessName(), type.getHumanName() + " Success");
      builder = builder.dataSource(metric.getUserErrorName(), type.getHumanName() + " User Error");
    }

    String name2 = "array_response_numitems";
    category.graph(getGraphName(name2))
        .muninName(getMuninName(group, TYPE, name2))
        .vlabel("# Items In Array Responses")
        .dataSource(eventsInJobHistory, "Events In Job History Response")
        .dataSource(jobsInJobList, "Jobs In Job List Response");
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
}

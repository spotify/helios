package com.spotify.helios.master.metrics;

import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import com.sun.jersey.spi.container.ResourceMethodDispatchAdapter;
import com.sun.jersey.spi.container.ResourceMethodDispatchProvider;

public class ReportingResourceMethodDispatchAdapter implements ResourceMethodDispatchAdapter {

  private final MasterMetrics metrics;

  public ReportingResourceMethodDispatchAdapter(final MasterMetrics metrics) {
    this.metrics = metrics;
  }

  @Override
  public ResourceMethodDispatchProvider adapt(final ResourceMethodDispatchProvider provider) {
    return new ReportingResourceMethodDispatchProvider(provider, metrics);
  }
}

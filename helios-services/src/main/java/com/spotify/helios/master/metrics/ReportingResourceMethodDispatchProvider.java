package com.spotify.helios.master.metrics;

import com.spotify.helios.servicescommon.statistics.MasterMetrics;
import com.sun.jersey.api.model.AbstractResourceMethod;
import com.sun.jersey.spi.container.ResourceMethodDispatchProvider;
import com.sun.jersey.spi.dispatch.RequestDispatcher;

public class ReportingResourceMethodDispatchProvider implements ResourceMethodDispatchProvider {

  private final ResourceMethodDispatchProvider provider;
  private final MasterMetrics metrics;

  public ReportingResourceMethodDispatchProvider(final ResourceMethodDispatchProvider provider,
                                                 final MasterMetrics metrics) {
    this.provider = provider;
    this.metrics = metrics;
  }

  @Override
  public RequestDispatcher create(final AbstractResourceMethod abstractResourceMethod) {
    final RequestDispatcher dispatcher = provider.create(abstractResourceMethod);
    return new ReportingResourceMethodDispatcher(dispatcher, metrics);
  }
}

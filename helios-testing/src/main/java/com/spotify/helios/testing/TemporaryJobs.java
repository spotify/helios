package com.spotify.helios.testing;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.spotify.helios.client.HeliosClient;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class TemporaryJobs extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(TemporaryJob.class);

  private static final String DEFAULT_USER = System.getProperty("user.name");
  private static final Prober DEFAULT_PROBER = new DefaultProber();

  private final HeliosClient client;
  private final Prober prober;

  private final List<TemporaryJob.Builder> builders = Lists.newArrayList();

  TemporaryJobs(final HeliosClient client, final Prober prober) {
    this.client = client;
    this.prober = prober;
  }

  public TemporaryJob.Builder job() {
    final TemporaryJob.Builder builder = new TemporaryJob.Builder(client, prober);
    builders.add(builder);
    return builder;
  }

  public static TemporaryJobs create() {
    final String domain = System.getProperty("HELIOS_DOMAIN");
    if (!Strings.isNullOrEmpty(domain)) {
      return create(domain);
    }
    final String endpoints = System.getProperty("HELIOS_ENDPOINTS");
    final HeliosClient.Builder clientBuilder = HeliosClient.newBuilder()
        .setUser(DEFAULT_USER);
    if (!Strings.isNullOrEmpty(endpoints)) {
      clientBuilder.setEndpointStrings(Splitter.on(',').splitToList(endpoints));
    } else {
      clientBuilder.setEndpoints("http://localhost:5801");
    }
    return create(clientBuilder.build());
  }

  public static TemporaryJobs create(final HeliosClient client) {
    return new TemporaryJobs(client, DEFAULT_PROBER);
  }

  public static TemporaryJobs create(final String domain) {
    return create(HeliosClient.create(domain, DEFAULT_USER));
  }

  @Override
  protected void after() {
    final List<AssertionError> errors = Lists.newArrayList();

    for (TemporaryJob.Builder builder : builders) {
      final TemporaryJob job = builder.job;
      if (job == null) {
        log.warn("deploy() not called on job");
        continue;
      }
      job.undeploy(errors);
    }

    for (AssertionError error : errors) {
      log.error(error.getMessage());
    }
  }
}

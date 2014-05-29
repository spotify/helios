package com.spotify.helios.testing;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.fail;

public class TemporaryJobs extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(TemporaryJob.class);

  private static final String DEFAULT_USER = System.getProperty("user.name");
  private static final Prober DEFAULT_PROBER = new DefaultProber();

  private final HeliosClient client;
  private final Prober prober;

  private final List<TemporaryJob> jobs = Lists.newArrayList();

  private final TemporaryJob.Deployer deployer = new TemporaryJob.Deployer() {
    @Override
    public TemporaryJob deploy(final Job job, final String hostFilter,
                               final Set<String> waitPorts) {
      if (isNullOrEmpty(hostFilter)) {
        fail("a host filter pattern must be passed to hostFilter(), or one must be specified in HELIOS_HOST_FILTER");
      }

      List<String> hosts = null;
      try {
        hosts = client.listHosts().get();
      } catch (InterruptedException | ExecutionException e) {
        fail(format("Failed to get list of Helios hosts - %s", e));
      }

      hosts = new ArrayList<>(Collections2.filter(hosts, new Predicate<String>() {
        @Override
        public boolean apply(@Nullable final String input) {
          return Pattern.matches(hostFilter, input);
        }
      }));

      if (hosts.isEmpty()) {
        fail(format("no hosts matched the filter pattern - %s", hostFilter));
      }

      String chosenHost = hosts.get(new Random().nextInt(hosts.size()));
      return deploy(job, asList(chosenHost), waitPorts);
    }

    @Override
    public TemporaryJob deploy(final Job job, final List<String> hosts,
                               final Set<String> waitPorts) {
      if (!started) {
        fail("deploy() must be called in a @Before or in the test method");
      }

      if (hosts.isEmpty()) {
        fail("at least one host must be explicitly specified, or deploy() must be called with no arguments to automatically select a host");
      }

      final TemporaryJob temporaryJob = new TemporaryJob(client, prober, job, hosts, waitPorts);
      jobs.add(temporaryJob);
      temporaryJob.deploy();
      return temporaryJob;
    }
  };

  private boolean started;

  TemporaryJobs(final HeliosClient client, final Prober prober) {
    this.client = client;
    this.prober = prober;
  }

  public TemporaryJobBuilder job() {
    return new TemporaryJobBuilder(deployer);
  }

  /**
   * Creates a new instance of TemporaryJobs. Will attempt to connect to a helios master at
   * http://localhost:5801 by default. This can be overridden by setting one of two environment
   * variables.
   * <ul>
   * <li>HELIOS_DOMAIN - any domain which contains a helios master</li>
   * <li>HELIOS_ENDPOINTS - a comma separated list of helios master endpoints</li>
   * </ul>
   * If both variables are set, HELIOS_DOMAIN will take precedence.
   * @return an instance of TemporaryJobs
   */

  public static TemporaryJobs create() {
    final String domain = System.getProperty("HELIOS_DOMAIN");
    if (!isNullOrEmpty(domain)) {
      return create(domain);
    }
    final String endpoints = System.getProperty("HELIOS_ENDPOINTS");
    final HeliosClient.Builder clientBuilder = HeliosClient.newBuilder()
        .setUser(DEFAULT_USER);
    if (!isNullOrEmpty(endpoints)) {
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
  protected void before() throws Throwable {
    started = true;
  }

  @Override
  protected void after() {
    final List<AssertionError> errors = Lists.newArrayList();

    for (TemporaryJob job : jobs) {
      job.undeploy(errors);
    }

    for (AssertionError error : errors) {
      log.error(error.getMessage());
    }
  }
}

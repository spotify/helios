package com.spotify.helios.servicescommon.statistics;

import com.google.common.base.Joiner;

import com.spotify.hermes.Hermes;
import com.spotify.statistics.JvmMetrics;
import com.spotify.statistics.MuninReporter;
import com.spotify.statistics.MuninReporterConfig;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Responsible for running various munin components and creating metric gathering classes
 * {@link ServiceMetrics}, {@link CachingStoreMetrics} and {@link CassandraMetrics}.
 */
public class MetricsImpl implements Metrics {

  private static final Logger log = LoggerFactory.getLogger(MetricsImpl.class);
  private static final String GROUP = "helios";
  private static final String CATEGORY_HERMES = "Hermes";
  private static final String CATEGORY_JVM = "JVM";
  private static final InetAddress DEFAULT_BIND_ADDRESS;
  private MuninReporter muninReporter;
  private MasterMetrics masterMetrics;
  private SupervisorMetrics supervisorMetrics;
  private final int muninReporterPort;

  static {
    try {
      DEFAULT_BIND_ADDRESS = InetAddress.getByAddress(new byte[]{127, 0, 0, 1});
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  public MetricsImpl(int muninReporterPort) {
    this.muninReporterPort = muninReporterPort;
  }

  @Override
  public void start() {
    // setup munin basics
    final MetricsRegistry registry = getRegistry();
    final MuninReporterConfig reporterConfig = new MuninReporterConfig(registry);

    // setup hermes and JVM metrics
    Hermes.configureServerGraphs(reporterConfig.category(CATEGORY_HERMES));
    JvmMetrics.register(registry, reporterConfig.category(CATEGORY_JVM));

    // setup JMX reporting
    JmxReporter.startDefault(registry);

    // agent, master and zookeeper metrics
    masterMetrics = new MasterMetricsImpl(GROUP, reporterConfig, registry);
    supervisorMetrics = new SupervisorMetricsImpl(GROUP, reporterConfig, registry);

    // start munin now that all the graphs have been defined
    if (muninReporterPort != 0) {
      muninReporter = new MuninReporter(registry, muninReporterPort, DEFAULT_BIND_ADDRESS,
          reporterConfig.build());
      log.info("Starting munin");
      muninReporter.start();
    }
  }

  @Override
  public void stop() {
    if (muninReporter != null) {
      muninReporter.shutdown();
    }
  }

  /**
   * Converts {@link com.yammer.metrics.core.MetricName} properties into a munin name. Joins
   * group, type, name and an optional suffix with underscores and converts to lowercase.
   * @param group {@code MetricName} group
   * @param type {@code MetricName} type
   * @param name {@code MetricName} name
   * @param suffix optional suffix, ignored if null
   * @return a properly formatted munin name
   */
  public static String getMuninName(String group, String type, String name, String suffix) {
    // join each string using underscore as separator and make lowercase
    return Joiner.on("_").skipNulls().join(group, type, name, suffix).toLowerCase();
  }

  /**
   * Converts {@link com.yammer.metrics.core.MetricName} properties into a munin name. Joins
   * group, type and name with underscores and converts to lowercase.
   * @param group {@code MetricName} group
   * @param type {@code MetricName} type
   * @param name {@code MetricName} name
   * @return a properly formatted munin name
   */
  public static String getMuninName(String group, String type, String name) {
    return getMuninName(group, type, name, null);
  }

  /**
   * Helper method for converting a munin name into a readable graph name. Replaces
   * underscores with spaces, ensures first letter is upper case, appends suffix
   * to end of graph name if not null.
   * @param muninName a munin name which will be converted into a graph name
   * @param suffix a string to be appended to the graph name. Ignored if null.
   * @return a nicely formatted graph name
   */
  public static String getGraphName(String muninName, String suffix) {
    String name = muninName;

    // append suffix to name if one was provided
    if (suffix != null) {
      name += " " + suffix;
    }

    // make first character uppercase
    String result = Character.toUpperCase(name.charAt(0)) + name.substring(1);

    // replace all munin underscores with spaces
    return result.replaceAll("_", " ");
  }

  /**
   * Helper method for converting a munin name into a readable graph name. Replaces
   * underscores with spaces, ensures first letter is upper case.
   * @param muninName a munin name which will be converted into a graph name
   * @return a nicely formatted graph name
   */
  public static String getGraphName(String muninName) {
    return getGraphName(muninName, null);
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

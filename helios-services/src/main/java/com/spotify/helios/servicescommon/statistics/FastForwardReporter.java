/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.servicescommon.statistics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.helios.common.Version;
import eu.toolchain.ffwd.FastForward;
import eu.toolchain.ffwd.Metric;
import io.dropwizard.lifecycle.Managed;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends the metrics in a v3 MetricRegistry to
 * <a href="https://github.com/spotify/ffwd">FastForward</a>.
 *
 * <p>The String "name" of the Metric is sent as the 'what' attribute in the FastForward metric. No
 * translation of the (possibly long, Java classname-ish) name is done. The type of metric is
 * included in the 'metric_type' attribute.</p>
 *
 * <p>For meters, timers, and histograms, the various component values of the
 * com.codahale.metrics.Metric (m1/m5/m15, p50, p99 etc) are sent as individual {@link Metric}
 * instances. The 'stat' attribute is filled out with the corresponding statistic. </p>
 *
 * <p>Note that not every statistic from Meters/Timers/Histograms is sent to FastForward in order
 * to limit the amount of data being sent and needing to be stored downstream. For Metered
 * instances, only 1m and 5m is sent. For Histograms, just median, p75 and p99 (plus
 * min/max/mean/stddev).
 * </p>
 */
public class FastForwardReporter implements Managed {

  private static final Logger log = LoggerFactory.getLogger(FastForwardReporter.class);

  /**
   * Create a new FastForwardReporter instance.
   *
   * @param registry        MetricRegistry to report to ffwd
   * @param address         Optional HostAndPort to override the defaults that ffwd client uses
   * @param metricKey       key to use for all metrics
   * @param intervalSeconds how often to report metrics to ffwd
   */
  public static FastForwardReporter create(
      MetricRegistry registry,
      Optional<HostAndPort> address,
      String metricKey,
      int intervalSeconds)
      throws SocketException, UnknownHostException {

    return create(registry, address, metricKey, intervalSeconds, Collections::emptyMap);
  }

  /**
   * Overload of {@link #create(MetricRegistry, Optional, String, int)} which allows for setting
   * additional attributes in each reported metric.
   *
   * <p>The additional attributes are modeled as a Supplier to allow for attributes that might
   * change values at runtime.
   */
  public static FastForwardReporter create(
      MetricRegistry registry,
      Optional<HostAndPort> address,
      String metricKey, int intervalSeconds,
      Supplier<Map<String, String>> additionalAttributes)
      throws SocketException, UnknownHostException {

    final FastForward ff;
    if (address.isPresent()) {
      ff = FastForward.setup(address.get().getHostText(), address.get().getPort());
    } else {
      ff = FastForward.setup();
    }

    final ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("fast-forward-reporter-%d")
        .build();

    final ScheduledExecutorService executorService =
        Executors.newSingleThreadScheduledExecutor(threadFactory);

    return new FastForwardReporter(ff, registry, executorService, metricKey, intervalSeconds,
        TimeUnit.SECONDS, additionalAttributes);
  }

  private final FastForward fastForward;
  private final MetricRegistry metricRegistry;
  private final ScheduledExecutorService executor;
  private final String key;
  private final long interval;
  private final TimeUnit intervalTimeUnit;
  private final Supplier<Map<String, String>> additionalAttributesSupplier;
  private Map<String, String> additionalAttributes;

  FastForwardReporter(FastForward fastForward, MetricRegistry metricRegistry,
                      ScheduledExecutorService executor, String key,
                      long interval, TimeUnit intervalTimeUnit,
                      Supplier<Map<String, String>> additionalAttributes) {
    this.fastForward = fastForward;
    this.metricRegistry = metricRegistry;
    this.executor = executor;
    this.key = key;
    this.interval = interval;
    this.intervalTimeUnit = intervalTimeUnit;
    this.additionalAttributesSupplier = additionalAttributes;
  }

  @Override
  public void start() throws Exception {
    log.info("Scheduling reporting of metrics every {} {}",
        interval, intervalTimeUnit.name().toLowerCase());

    // wrap the runnable in a try-catch as uncaught exceptions will prevent subsequent executions
    executor.scheduleAtFixedRate(() -> {
      try {
        reportOnce();
      } catch (Exception e) {
        log.error("Exception in reporting loop", e);
      }
    }, interval, interval, intervalTimeUnit);
  }

  @Override
  public void stop() throws Exception {
    executor.shutdown();
  }

  @VisibleForTesting
  void reportOnce() {

    // resolve the additional attributes once per report
    additionalAttributes = additionalAttributesSupplier.get();
    if (additionalAttributes == null) {
      additionalAttributes = Collections.emptyMap();
    }

    metricRegistry.getGauges().forEach(this::reportGauge);

    metricRegistry.getCounters().forEach(this::reportCounter);

    metricRegistry.getMeters().forEach(this::reportMeter);

    metricRegistry.getHistograms().forEach(this::reportHistogram);

    metricRegistry.getTimers().forEach(this::reportTimer);
  }

  private void reportGauge(String name, Gauge gauge) {
    final Metric metric = createMetric(name, "gauge")
        .value(convert(gauge.getValue()));
    send(metric);
  }

  private void reportCounter(String name, Counter counter) {
    final Metric metric = createMetric(name, "counter")
        .value(counter.getCount());
    send(metric);
  }

  private void reportMeter(String name, Meter meter) {
    final Metric metric = createMetric(name, "meter")
        .attribute("unit", "n/s");

    reportMetered(metric, meter);
  }

  private void reportMetered(Metric metric, Metered metered) {
    //purposefully don't emit 15m as it is not very useful
    send(metric.attribute("stat", "1m").value(metered.getOneMinuteRate()));
    send(metric.attribute("stat", "5m").value(metered.getOneMinuteRate()));
  }

  private void reportHistogram(String name, Histogram histogram) {
    final Metric metric = createMetric(name, "histogram");
    reportHistogram(metric, histogram.getSnapshot());
  }

  private void reportHistogram(Metric metric, Snapshot snapshot) {
    send(metric.attribute("stat", "min").value(snapshot.getMin()));
    send(metric.attribute("stat", "max").value(snapshot.getMax()));
    send(metric.attribute("stat", "mean").value(snapshot.getMean()));
    send(metric.attribute("stat", "stddev").value(snapshot.getStdDev()));

    send(metric.attribute("stat", "median").value(snapshot.getMedian()));
    send(metric.attribute("stat", "p75").value(snapshot.get75thPercentile()));
    send(metric.attribute("stat", "p99").value(snapshot.get99thPercentile()));
  }

  private void reportTimer(String name, Timer timer) {
    final Metric metric = createMetric(name, "timer")
        .attribute("unit", "ns");

    reportHistogram(metric, timer.getSnapshot());
    reportMetered(metric, timer);
  }

  private Metric createMetric(String metricName, String metricType) {
    return FastForward.metric(key)
        .attributes(additionalAttributes)
        .attribute("helios_version", Version.POM_VERSION)
        .attribute("metric_type", metricType)
        .attribute("what", metricName);
  }

  private double convert(Object value) {
    if (value instanceof Number) {
      return Number.class.cast(value).doubleValue();
    }

    if (value instanceof Boolean) {
      return (Boolean) value ? 1 : 0;
    }

    return 0;
  }

  private void send(Metric metric) {
    try {
      fastForward.send(metric);
    } catch (IOException e) {
      log.error("Error sending metric to FastForward", e);
    }
  }
}

/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.servicescommon.statistics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.spotify.helios.common.Version;

import eu.toolchain.ffwd.FastForward;
import eu.toolchain.ffwd.Metric;

import org.hamcrest.CoreMatchers;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;

public class FastForwardReporterTest {

  private static final int REPORTING_INTERVAL_MILLIS = 10;
  private static final int VERIFY_TIMEOUT = REPORTING_INTERVAL_MILLIS * 2;

  private final FastForward ffwd = mock(FastForward.class);
  private final MetricRegistry metricRegistry = new MetricRegistry();

  private FastForwardReporter reporter;

  @Before
  public void setUp() throws Exception {
    final ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).build();

    final ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(threadFactory);

    this.reporter = new FastForwardReporter(ffwd, metricRegistry, executor, "helios.test",
                                            REPORTING_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
  }

  @After
  public void tearDown() throws Exception {
    reporter.stop();
  }

  // shortcut to Mockito.verify(ffwd, timeout(..).times(..))...
  private FastForward verify() {
    return Mockito.verify(ffwd, timeout(VERIFY_TIMEOUT).atLeastOnce());
  }

  /**
   * A matcher that matches when the actual object contains all of the given attributes
   * (note that the reverse isn't necessarily true; this is a partial matcher).
   */
  private static CustomTypeSafeMatcher<Metric> containsAttributes(Map<String, String> attributes) {

    final String description = String.format("a metric containing attributes=%s", attributes);

    return new CustomTypeSafeMatcher<Metric>(description) {
      @Override
      protected boolean matchesSafely(final Metric item) {
        return item.getAttributes().entrySet().containsAll(attributes.entrySet());
      }
    };
  }

  private static CustomTypeSafeMatcher<Metric> containsAttributes(String... strings) {
    final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (int i = 0; i < strings.length; i += 2) {
      builder.put(strings[i], strings[i + 1]);
    }
    return containsAttributes(builder.build());
  }

  private static CustomTypeSafeMatcher<Metric> hasValue(double value) {
    return new CustomTypeSafeMatcher<Metric>("a metric with value=" + value) {
      @Override
      protected boolean matchesSafely(final Metric item) {
        return item.getValue() == value;
      }
    };
  }

  private static CustomTypeSafeMatcher<Metric> hasKey(String key) {
    return new CustomTypeSafeMatcher<Metric>("a metric with key=" + key) {
      @Override
      protected boolean matchesSafely(final Metric item) {
        return item.getKey().equals(key);
      }
    };
  }

  // The compiler has a very hard time inferring the type of an expression like
  // argThat(allOf(foo(), bar())) as being Matcher<Metric>, so we define the method here
  // to help out poor confused javac.
  @SafeVarargs
  private static Matcher<Metric> allOf(Matcher<Metric>... matchers) {
    return CoreMatchers.allOf(matchers);
  }

  @Test
  public void testGauges() throws Exception {
    metricRegistry.register("some.gauge1", (Gauge<Integer>) () -> 1);
    metricRegistry.register("some.gauge2", (Gauge<Integer>) () -> 2);

    reporter.start();

    verify().send(argThat(allOf(
        hasKey("helios.test"),
        containsAttributes("what", "some.gauge1", "metric_type", "gauge"),
        hasValue(1)
    )));

    verify().send(argThat(allOf(
        hasKey("helios.test"),
        containsAttributes("what", "some.gauge2", "metric_type", "gauge"),
        hasValue(2)
    )));
  }

  @Test
  public void testCounter() throws Exception {
    metricRegistry.counter("counting.is.fun")
        .inc(7982);

    reporter.start();

    verify().send(argThat(allOf(
        hasKey("helios.test"),
        containsAttributes("what", "counting.is.fun", "metric_type", "counter"),
        hasValue(7982)
    )));
  }

  @Test
  public void testMeter() throws Exception {
    metricRegistry.meter("the-meter");

    reporter.start();

    verifyMeterStats("the-meter", "meter");
  }

  private void verifyMeterStats(String what, String type) throws Exception {
    for (String stat : new String[]{"1m", "5m"}) {
      verify().send(argThat(allOf(
          hasKey("helios.test"),
          containsAttributes("what", what, "metric_type", type, "stat", stat),
          hasValue(0)
      )));
    }
  }

  @Test
  public void testHistogram() throws Exception {
    final Histogram h = metricRegistry.histogram("histo.gram");
    IntStream.range(1, 10).forEach(h::update);

    reporter.start();

    verifyHistogramStats("histo.gram", "histogram");
  }

  private void verifyHistogramStats(String what, String type) throws Exception {
    final Set<String> expectedStats =
        ImmutableSet.of("median", "p75", "p99", "mean", "min", "max", "stddev");

    for (String stat : expectedStats) {
      verify().send(argThat(allOf(
          hasKey("helios.test"),
          containsAttributes("what", what, "metric_type", type, "stat", stat))));
    }
  }

  @Test
  public void testTimer() throws Exception {
    metricRegistry.timer("blah-timer");

    reporter.start();

    verifyHistogramStats("blah-timer", "timer");
    verifyMeterStats("blah-timer", "timer");
  }

  @Test
  public void testAttributesIncludeHeliosVersion() throws Exception {
    metricRegistry.register("something", (Gauge<Integer>) () -> 1);

    reporter.start();

    verify().send(argThat(containsAttributes("helios_version", Version.POM_VERSION)));
  }
}

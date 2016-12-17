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

package com.spotify.helios.servicescommon;

import com.google.cloud.ByteArray;
import com.google.cloud.pubsub.Message;
import com.google.cloud.pubsub.PubSub;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

/** An EventSender that publishes events to Google Cloud PubSub. */
public class GooglePubSubSender implements EventSender {

  private static final Logger log = LoggerFactory.getLogger(GooglePubSubSender.class);

  private final PubSub pubsub;
  private final String topicPrefix;
  private final HealthChecker healthchecker;

  public static GooglePubSubSender create(final PubSub pubSub,
                                          final String topicPrefix,
                                          final HealthChecker healthchecker) {
    return new GooglePubSubSender(pubSub, topicPrefix, healthchecker);
  }

  private GooglePubSubSender(final PubSub pubSub,
                             final String topicPrefix,
                             final HealthChecker healthchecker) {
    this.pubsub = pubSub;
    this.topicPrefix = topicPrefix;
    this.healthchecker = healthchecker;
  }

  @Override
  public void start() throws Exception {
    healthchecker.start();
  }

  @Override
  public void stop() throws Exception {
    healthchecker.stop();
  }

  @Override
  public void send(final String topic, final byte[] message) {
    final String combinedTopic = topicPrefix + topic;

    if (!healthchecker.isHealthy()) {
      log.warn("will not publish message to pubsub topic={} as the pubsub client "
               + "appears to be unhealthy", combinedTopic);
      return;
    }

    try {
      Futures.addCallback(
          JdkFutureAdapters.listenInPoolThread(
              pubsub.publishAsync(combinedTopic, Message.of(ByteArray.copyFrom(message)))),
          new FutureCallback<String>() {
            @Override
            public void onSuccess(@Nullable final String ackId) {
              log.debug("Sent an event to Google PubSub, topic: {}, ack: {}", combinedTopic, ackId);
            }

            @Override
            public void onFailure(final Throwable t) {
              log.warn("Unable to send an event to Google PubSub, topic: {}", combinedTopic, t);
            }
          });
    } catch (Exception e) {
      log.warn("Failed to publish Google PubSub message, topic: {}", combinedTopic, e);
    }
  }

  public interface HealthChecker extends Managed {

    boolean isHealthy();
  }

  public static class DefaultHealthChecker implements HealthChecker {

    private final PubSub pubsub;
    private final String topic;
    private final ScheduledExecutorService executor;
    private final Duration healthcheckInterval;
    private AtomicBoolean healthy = new AtomicBoolean(false);

    public DefaultHealthChecker(final PubSub pubsub, final String topic,
                                final ScheduledExecutorService executor,
                                final Duration healthcheckInterval) {
      this.pubsub = pubsub;
      this.topic = topic;
      this.executor = executor;
      this.healthcheckInterval = healthcheckInterval;
    }

    @Override
    public void start() {
      final long millis = healthcheckInterval.toMillis();
      executor.scheduleWithFixedDelay(this::checkHealth, 0, millis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() throws Exception {
      executor.shutdown();
    }

    @Override
    public boolean isHealthy() {
      return healthy.get();
    }

    @VisibleForTesting
    void checkHealth() {
      healthy.set(doCheckHealth());
    }

    private boolean doCheckHealth() {
      try {
        // perform a blocking call to see if we can connect to pubsub at all
        // if the topic does not exist, getTopic() returns null and does not throw an exception
        pubsub.getTopic(topic);
        log.info("successfully checked if pubsub topic {} exists - this instance is healthy",
            topic);
        return true;
      } catch (RuntimeException ex) {
        // PubSubException is an instance of RuntimeException, catch any other subtypes too
        log.warn("caught exception checking if pubsub topic {} exists. "
                 + "Publishing to pubsub will be disabled until connectivity is restored "
                 + "(next check is in {})",
            topic, healthcheckInterval, ex);
        return false;
      }
    }

  }
}

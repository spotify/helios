/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.helios.master.reaper;

import static com.google.common.base.Preconditions.checkArgument;

import com.spotify.helios.common.Clock;
import com.spotify.helios.common.SystemClock;
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.master.MasterModel;

import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.ReceivedMessage;
import com.google.cloud.pubsub.SubscriptionInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.logging.v2.LogEntry;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * De-registers agents when the underlying Google Compute Engine instance is deleted.
 * To use this, create a sink that exports GCE activity logs to a Google PubSub topic:
 * https://cloud.google.com/logging/docs/export/configure_export_v2#creating_sinks
 *
 * This reaper will read messages from the PubSub topic and unregister agents as soon
 * as the delete operation is completed.
 */
public class GceAgentReaper extends RateLimitedService<ReceivedMessage> {

  private static final double PERMITS_PER_SECOND = 0.2; // one permit every 5 seconds
  private static final Clock SYSTEM_CLOCK = new SystemClock();
  private static final int DELAY = 5;
  private static final TimeUnit TIME_UNIT = TimeUnit.MINUTES;

  private static final String SUBSCRIPTION = GceAgentReaper.class.getName();

  private static final Logger log = LoggerFactory.getLogger(GceAgentReaper.class);

  private final MasterModel masterModel;
  private final PubSub pubsub;
  private final String topic;
  private final long timeoutMillis;
  private final Clock clock;

  public GceAgentReaper(final MasterModel masterModel,
                        final long timeoutHours,
                        final PubSub pubsub,
                        final String topic) {
    this(masterModel, pubsub, topic, timeoutHours, SYSTEM_CLOCK, PERMITS_PER_SECOND,
         new Random().nextInt(DELAY));
  }

  @VisibleForTesting
  GceAgentReaper(final MasterModel masterModel,
                 final PubSub pubsub,
                 final String topic,
                 final long timeoutHours,
                 final Clock clock,
                 final double permitsPerSecond,
                 final int initialDelay) {
    super(permitsPerSecond, initialDelay, DELAY, TIME_UNIT);
    this.masterModel = masterModel;
    this.pubsub = pubsub;
    this.topic = topic;
    checkArgument(timeoutHours > 0);
    this.timeoutMillis = TimeUnit.HOURS.toMillis(timeoutHours);
    this.clock = clock;
  }

  @Override
  protected void startUp() throws Exception {
    super.startUp();

    if (pubsub.getSubscription(SUBSCRIPTION) == null) {
      final SubscriptionInfo subscriptionInfo = SubscriptionInfo.newBuilder(topic, SUBSCRIPTION)
          // put the message back in the queue if it's not consumed within one iteration
          .setAckDeadLineSeconds((int) TIME_UNIT.convert(DELAY, TimeUnit.SECONDS))
          .build();
      pubsub.create(subscriptionInfo);
    }
  }

  @Override
  Iterable<ReceivedMessage> collectItems() {
    // only pull as many messages as we can process in one iteration
    final int maxMessages = (int) (TIME_UNIT.convert(DELAY, TimeUnit.SECONDS) / PERMITS_PER_SECOND);
    return ImmutableList.copyOf(pubsub.pull(SUBSCRIPTION, maxMessages));
  }

  @Override
  void processItem(final ReceivedMessage message) {
    try {
      // we ack before processing. something could still go wrong, but it's preferable to miss
      // a few deregistrations that should then be caught by DeadAgentReaper rather than block
      // the queue due to exceptions
      message.ack();

      final LogEntry logEntry = LogEntry.parseFrom(message.getPayload().asInputStream());
      final Struct payload = logEntry.getJsonPayload();

      if (!"GCE_OPERATION_DONE".equals(payload.getFieldsOrThrow("event_type").getStringValue())) {
        return;
      }
      if (!"compute.instances.delete".equals(payload.getFieldsOrThrow("event_subtype")
                                                 .getStringValue())) {
        return;
      }

      final String host = logEntry.getResource().getLabelsOrThrow("name");

      if (masterModel.isHostUp(host)) {
        // Host UP -- nothing to do
        return;
      }

      final AgentInfo agentInfo = masterModel.getAgentInfo(host);
      if (agentInfo == null) {
        return;
      }

      try {
        log.info("Reaping deleted GCE instance '{}'");
        masterModel.deregisterHost(host);
      } catch (Exception e) {
        log.warn("Failed to reap deleted GCE instance '{}'", host, e);
      }
    } catch (Exception e) {
      log.warn("Failed to parse pubsub message: {}", message, e);
    }
  }
}

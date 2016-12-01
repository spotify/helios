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

package com.spotify.helios.servicescommon;

import com.google.cloud.ByteArray;
import com.google.cloud.pubsub.Message;
import com.google.cloud.pubsub.PubSub;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class GooglePubSubSender implements EventSender {
  private static final Logger log = LoggerFactory.getLogger(GooglePubSubSender.class);

  private final PubSub pubsub;
  private final String topicPrefix;

  public GooglePubSubSender(final PubSub pubSub, final String topicPrefix) {
    this.pubsub = pubSub;
    this.topicPrefix = topicPrefix;
  }

  @Override
  public boolean isHealthy() {
    final String topic = topicPrefix + "canary";
    try {
      // perform a blocking call to see if we can connect to pubsub at all
      // if the topic does not exist, this method returns null and does not throw an exception
      pubsub.getTopic(topic);
      log.info("successfully checked if topic {} exists - this instance is healthy", topic);
      return true;
    } catch (RuntimeException ex) {
      // PubSubException is an instance of RuntimeException, catch any other subtypes too
      log.warn("caught exception checking if topic {} exists - this instance is unhealthy",
          topic, ex);
      return false;
    }
  }

  @Override
  public void send(final String topic, final byte[] message) {
    final String combinedTopic = topicPrefix + topic;
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
}

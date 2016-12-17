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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.cloud.pubsub.PubSub;
import com.google.cloud.pubsub.PubSubOptions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.Environment;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.kafka.clients.producer.KafkaProducer;

public final class EventSenderFactory {

  private EventSenderFactory() {
  }

  public static List<EventSender> build(
      final Environment environment,
      final CommonConfiguration<?> config,
      final MetricRegistry metricRegistry, final String pubsubHealthcheckTopic) {

    final List<EventSender> senders = new ArrayList<>();

    final KafkaClientProvider kafkaClientProvider =
        new KafkaClientProvider(config.getKafkaBrokers());

    final Optional<KafkaProducer<String, byte[]>> kafkaProducer =
        kafkaClientProvider.getDefaultProducer();

    kafkaProducer.ifPresent(producer -> senders.add(new KafkaSender(producer)));

    final LifecycleEnvironment lifecycle = environment.lifecycle();

    if (!config.getPubsubPrefixes().isEmpty()) {
      final PubSub pubsub = PubSubOptions.getDefaultInstance().getService();

      final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("pubsub-healthchecker-%d")
              .build()
      );

      // choose an arbitrary prefix to use in the healthcheck. we assume if we can connect to
      // one we can connect to all
      final String topicToHealthcheck =
          config.getPubsubPrefixes().iterator().next() + pubsubHealthcheckTopic;

      final GooglePubSubSender.DefaultHealthChecker healthchecker =
          new GooglePubSubSender.DefaultHealthChecker(pubsub, topicToHealthcheck, executor,
              Duration.ofMinutes(5));

      metricRegistry.register("pubsub-health", (Gauge<Boolean>) healthchecker::isHealthy);

      for (final String prefix : config.getPubsubPrefixes()) {
        final GooglePubSubSender sender = GooglePubSubSender.create(pubsub, prefix, healthchecker);
        senders.add(sender);
      }

      lifecycle.manage(new ManagedPubSub(pubsub));
    }

    senders.forEach(lifecycle::manage);

    return senders;
  }

  /** Small wrapper so we can close the PubSub instance when service shuts down. */
  private static final class ManagedPubSub implements Managed {

    private final PubSub pubsub;

    private ManagedPubSub(final PubSub pubsub) {
      this.pubsub = pubsub;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
      pubsub.close();
    }
  }
}

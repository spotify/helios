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

import com.google.cloud.pubsub.Message;
import com.google.cloud.pubsub.PubSub;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GooglePubSubProviderTest {
  Supplier<PubSub> throwingSupplier;
  PubSub mockPubsub;

  @Before
  public void setUp() {
    throwingSupplier = () -> { throw new RuntimeException("Throwing supplier called"); };
    mockPubsub = mock(PubSub.class);
  }

  @Test
  public void testNoSendersForNullList() {
    final GooglePubSubProvider provider = new GooglePubSubProvider(null);
    assertThat(provider.senders(throwingSupplier).isEmpty(), is(true));
  }

  @Test
  public void testNoSendersForEmptyList() {
    final GooglePubSubProvider provider = new GooglePubSubProvider(emptyList());
    assertThat(provider.senders(throwingSupplier).isEmpty(), is(true));
  }

  @Test
  public void testSenderForPrefix() throws Exception {
    final GooglePubSubProvider provider = new GooglePubSubProvider(singletonList("prefix."));
    final List<GooglePubSubSender> senders = provider.senders(() -> mockPubsub);
    assertThat(senders.size(), is(1));

    senders.get(0).send("topic", new byte[] {'x'});
    verify(mockPubsub).publishAsync(eq("prefix.topic"), any(Message.class));
  }
}

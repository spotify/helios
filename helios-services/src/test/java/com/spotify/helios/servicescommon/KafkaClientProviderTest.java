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

package com.spotify.helios.servicescommon;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaClientProviderTest {

  @Test
  public void testNoBrokersConfigured() {
    final KafkaClientProvider provider = new KafkaClientProvider(null);

    assertEquals("KafkaClientProvider should return absent when null list of seed hosts is passed",
        Optional.absent(), provider.getDefaultProducer());
  }

  @Test
  public void testReturnsProvider() {
    final KafkaClientProvider provider = new KafkaClientProvider(ImmutableList.of("localhost"));

    assertTrue("Expected KafkaProvider to return non-absent KafkaProducer "
               + "when passed a list of seed hosts",
        provider.getDefaultProducer().isPresent());
  }
}

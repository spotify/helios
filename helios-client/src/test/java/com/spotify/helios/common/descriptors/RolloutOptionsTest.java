/*-
 * -\-\-
 * Helios Client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.helios.common.descriptors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.spotify.helios.common.Json;
import org.junit.Test;

public class RolloutOptionsTest {

  /**
   * Test that a JSON representing a RolloutOptions instance prior to the addition of the
   * ignoreFailures field can be deserialized properly.
   */
  @Test
  public void testCanDeserializeWithoutIgnoreFailuresField() throws Exception {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode node = mapper.createObjectNode()
        .put("migrate", false)
        .put("parallalism", 2)
        .put("timeout", 1000)
        .put("overlap", true)
        .put("token", "blah");

    final RolloutOptions options = Json.read(node.toString(), RolloutOptions.class);

    assertThat("RolloutOptions.ignoreFailures should default to null",
        options.getIgnoreFailures(), is(nullValue()));
  }

  @Test
  public void testCanDeserializeStringlyTypedFields() throws Exception {
    final ObjectMapper mapper = new ObjectMapper();
    final ObjectNode node = mapper.createObjectNode()
        .put("migrate", "false")
        .put("parallelism", "2")
        .put("timeout", "1000");

    final RolloutOptions options = Json.read(node.toString(), RolloutOptions.class);

    assertThat(options.getMigrate(), is(Boolean.FALSE));
    assertThat(options.getParallelism(), is(2));
    assertThat(options.getTimeout(), is(1000L));
  }

  @Test
  public void testCanDeserializeNullFields() throws Exception {
    final String json = "{\"migrate\": null, \"parallelism\": null}";

    final RolloutOptions options = Json.read(json, RolloutOptions.class);

    assertThat(options.getMigrate(), is(nullValue()));
    assertThat(options.getParallelism(), is(nullValue()));
  }

  @Test
  public void testRolloutOptionsWithFallback() throws Exception {
    final RolloutOptions allNull = RolloutOptions.newBuilder()
        .build();
    assertThat(allNull.withFallback(RolloutOptions.getDefault()),
        equalTo(RolloutOptions.getDefault()));

    final RolloutOptions partial = allNull.toBuilder()
        .setTimeout(1000L)
        .setParallelism(2)
        .setOverlap(true)
        .build();
    assertThat(partial.withFallback(RolloutOptions.getDefault()),
        equalTo(RolloutOptions.getDefault().toBuilder()
            .setTimeout(1000L)
            .setParallelism(2)
            .setOverlap(true)
            .build()));
  }
}

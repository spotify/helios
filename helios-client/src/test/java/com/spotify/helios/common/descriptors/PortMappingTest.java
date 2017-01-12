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

package com.spotify.helios.common.descriptors;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;
import static com.spotify.helios.common.descriptors.PortMapping.TCP;
import static com.spotify.helios.common.descriptors.PortMapping.UDP;
import static com.spotify.helios.common.descriptors.PortMapping.WILDCARD_ADDRESS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.io.Resources;
import com.spotify.helios.common.Json;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class PortMappingTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testJsonDeserializationFromFull() throws Exception {
    final PortMapping pm = portMappingFromResource("portmapping-full.json");
    assertThat(pm, equalTo(PortMapping.builder()
        .ip("1.2.3.4")
        .externalPort(123)
        .internalPort(456)
        .protocol(UDP)
        .build()));
  }

  @Test
  public void testJsonDeserializationFromPartial() throws Exception {
    final PortMapping pm = portMappingFromResource("portmapping-partial.json");
    assertThat(pm, equalTo(PortMapping.builder()
        .ip(WILDCARD_ADDRESS)
        .externalPort(123)
        .internalPort(456)
        .protocol(TCP)
        .build()));
  }

  @Test
  public void testJsonDeserializationFromInternalPortOnly() throws Exception {
    final PortMapping pm = portMappingFromResource("portmapping-internal-port-only.json");
    assertThat(pm, equalTo(PortMapping.builder()
        .ip(WILDCARD_ADDRESS)
        .internalPort(456)
        .protocol(TCP)
        .build()));
  }

  @Test
  public void testJsonDeserializationInvalidIp() throws Exception {
    expectedException.expect(JsonMappingException.class);
    portMappingFromResource("portmapping-invalid-ip.json");
  }

  @Test
  public void testJsonDeserializationUnknownFields() throws Exception {
    final PortMapping pm = portMappingFromResource("portmapping-unknown-fields.json");
    assertThat(pm, equalTo(PortMapping.builder()
        .ip(WILDCARD_ADDRESS)
        .internalPort(456)
        .externalPort(123)
        .protocol(TCP)
        .build()));
  }

  @Test
  public void testJsonSerialization() throws Exception {
    final PortMapping pm = PortMapping.builder()
        .ip("1.2.3.4")
        .internalPort(456)
        .externalPort(123)
        .protocol(UDP)
        .build();
    final String json = Json.asPrettyString(pm);
    assertThat(json, equalTo(stringFromResource("portmapping-full.json")));
  }

  @Test
  public void testJsonSerializationWithDefaults() throws Exception {
    final PortMapping pm = PortMapping.builder()
        .internalPort(456)
        .externalPort(123)
        .build();
    final String json = Json.asPrettyString(pm);
    assertThat(json, equalTo(stringFromResource("portmapping-defaults.json")));
  }

  @Test
  public void testJsonSerializationFromInternalPortOnly() throws Exception {
    final PortMapping pm = PortMapping.builder()
        .internalPort(456)
        .build();
    final String json = Json.asPrettyString(pm);
    assertThat(json, equalTo(stringFromResource("portmapping-serialized-internal-port-only.json")));
  }

  private PortMapping portMappingFromResource(final String resourceFile) throws IOException {
    return Json.read(stringFromResource(resourceFile), PortMapping.class);
  }

  private String stringFromResource(final String resourceFile) throws IOException {
    return Resources.toString(getResource(resourceFile), UTF_8).trim();
  }
}
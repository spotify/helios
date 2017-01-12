/*-
 * -\-\-
 * Helios Tools
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

package com.spotify.helios.cli.command;

import static com.google.common.base.Optional.fromNullable;
import static com.spotify.helios.common.descriptors.PortMapping.TCP;
import static java.util.regex.Pattern.compile;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.spotify.helios.common.descriptors.PortMapping;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


class PortMappingParser {

  private PortMappingParser() {
    // Prevent instantiation
  }

  // TODO (dxia) It'd be great to not have such a specific regex for the "ip" capture group,
  // and let `isInetAddress()` do all the validation. Without the specific regex, however,
  // it's hard to stay backwards compatible. Any ideas on a regex that'll work?
  private static final Pattern PATTERN =
      compile("(?<n>[_\\-\\w]+)=((?<ip>([0-9]{1,3}.){3}[0-9]{1,3}):)"
              + "?(?<i>\\d+)(:(?<e>\\d+))?(/(?<p>\\w+))?");

  static PortMappingWithName parsePortMapping(final String portSpec) {
    final Matcher matcher = PATTERN.matcher(portSpec);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Bad port mapping: " + portSpec);
    }

    final String name = matcher.group("n");
    final String ip = matcher.group("ip");
    final int internal = Integer.parseInt(matcher.group("i"));
    final Integer external = nullOrInteger(matcher.group("e"));
    final String protocol = fromNullable(matcher.group("p")).or(TCP);

    return PortMappingWithName.create(name, PortMapping.builder()
        .ip(ip)
        .internalPort(internal)
        .externalPort(external)
        .protocol(protocol)
        .build());
  }

  static Map<String, PortMapping> parsePortMappings(final List<String> portSpecs) {
    final Map<String, PortMapping> explicitPorts = Maps.newHashMap();

    for (final String spec : portSpecs) {
      final PortMappingWithName portMappingWithName = parsePortMapping(spec);

      final String name = portMappingWithName.name();
      if (explicitPorts.containsKey(name)) {
        throw new IllegalArgumentException("Duplicate port mapping name: " + name);
      }

      explicitPorts.put(name, portMappingWithName.portMapping());
    }

    return ImmutableMap.copyOf(explicitPorts);
  }

  /**
   * A class that simply lets us put the port name and {@link PortMapping} in one object.
   */
  static class PortMappingWithName {
    private final String name;
    private final PortMapping portMapping;

    private PortMappingWithName(final String name, final PortMapping portMapping) {
      this.name = name;
      this.portMapping = portMapping;
    }

    static PortMappingWithName create(final String name, final PortMapping portMapping) {
      return new PortMappingWithName(name, portMapping);
    }

    String name() {
      return name;
    }

    PortMapping portMapping() {
      return portMapping;
    }
  }

  private static Integer nullOrInteger(final String str) {
    return str == null ? null : Integer.valueOf(str);
  }
}

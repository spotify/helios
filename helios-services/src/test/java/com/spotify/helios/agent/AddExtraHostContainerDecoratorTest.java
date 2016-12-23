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

package com.spotify.helios.agent;

import static com.spotify.helios.agent.AddExtraHostContainerDecorator.isValidArg;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.docker.client.messages.HostConfig;
import java.util.List;
import org.junit.Test;

public class AddExtraHostContainerDecoratorTest {

  @Test
  public void simpleTest() {
    final List<String> hosts = ImmutableList.of("one", "two");
    final AddExtraHostContainerDecorator decorator = new AddExtraHostContainerDecorator(hosts);

    final HostConfig.Builder hostBuilder = HostConfig.builder();
    decorator.decorateHostConfig(null, Optional.absent(), hostBuilder);

    final HostConfig config = hostBuilder.build();
    assertThat(config.extraHosts(), equalTo(hosts));
  }

  @Test
  public void invalidArgument() {
    assertThat(isValidArg("abcdefg"), is(false));
  }

  @Test
  public void invalidArguments() {
    assertThat(isValidArg("abc:def:gh"), is(false));
  }

  @Test
  public void testValidArgument() {
    assertThat(isValidArg("foo:169.254.169.254"), is(true));
  }
}

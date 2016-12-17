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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;

import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.ImageInfo;
import com.spotify.helios.common.descriptors.Job;

import java.util.List;

/** ContainerDecorator that adds to {@link HostConfig#extraHosts}. */
public class AddExtraHostContainerDecorator implements ContainerDecorator {

  private final List<String> extraHosts;

  public AddExtraHostContainerDecorator(final List<String> extraHosts) {
    this.extraHosts = ImmutableList.copyOf(extraHosts);
  }

  @Override
  public void decorateHostConfig(final Job job, final Optional<String> dockerVersion,
                                 final HostConfig.Builder hostConfig) {
    hostConfig.extraHosts(this.extraHosts);
  }

  @Override
  public void decorateContainerConfig(final Job job, final ImageInfo imageInfo,
                                      final Optional<String> dockerVersion,
                                      final ContainerConfig.Builder containerConfig) {
    //do nothing
  }

  /**
   * Validates that the argument has form "host:ip-address". Does not validate the form of the
   * hostname argument, but checks that the second half is valid via {@link
   * InetAddresses#isInetAddress}.
   */
  public static boolean isValidArg(String arg) {
    final String[] args = arg.split(":");
    if (args.length != 2) {
      return false;
    }

    final String ip = args[1];
    return InetAddresses.isInetAddress(ip);
  }
}

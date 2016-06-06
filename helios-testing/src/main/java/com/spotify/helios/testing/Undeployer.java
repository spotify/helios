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

package com.spotify.helios.testing;

import com.spotify.helios.common.descriptors.Job;

import java.util.List;

/**
 * An interface that defines how a {@link TemporaryJob} is undeployed.
 * This interface exists solely for unit testing purposes.
 */
interface Undeployer {

  /**
   * Undeploy the job from all specified hosts, and delete the job. Any failures will be ignored,
   * and we will keep trying each host. A list of errors encountered along the way will be returned
   * to the caller.
   * @param job the {@link TemporaryJob} to undeploy and delete
   * @param hosts the hosts to undeploy from
   * @return A list of {@link AssertionError}, if any, that occurred.
   */
  List<AssertionError> undeploy(Job job, List<String> hosts);

}

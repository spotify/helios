/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import com.spotify.docker.client.LogMessage;
import com.spotify.helios.common.descriptors.JobId;
import java.io.IOException;
import java.util.Iterator;

/**
 * Follows a log stream in a blocking fashion.
 */
public interface LogStreamFollower {

  /**
   * Follows the specified log stream until it doesn't have any more log messages available.
   *
   * @param jobId       the job id that the log stream belongs to
   * @param containerId the container id that the log stream belongs to
   * @param logStream   the log stream to follow
   *
   * @throws IOException if an exception occurred
   */
  void followLog(JobId jobId, String containerId, Iterator<LogMessage> logStream)
      throws IOException;

}

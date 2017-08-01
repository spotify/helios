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

package com.spotify.helios.master;

import static java.lang.String.format;

import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.descriptors.JobId;

public class JobPortAllocationConflictException extends HeliosException {

  private final JobId id;
  private final JobId conflictingId;
  private final String host;
  private final int port;

  public JobPortAllocationConflictException(final JobId id, final JobId conflictingId,
                                            final String host, final int port) {

    super(format("Allocation of port %d for job %s collides with job %s on host %s",
        port, id, conflictingId, host));
    this.id = id;
    this.conflictingId = conflictingId;
    this.host = host;
    this.port = port;
  }

  public JobId getId() {
    return id;
  }

  public JobId getConflictingId() {
    return conflictingId;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }
}

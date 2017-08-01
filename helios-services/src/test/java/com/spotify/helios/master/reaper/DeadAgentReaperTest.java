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

package com.spotify.helios.master.reaper;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.spotify.helios.common.Clock;
import com.spotify.helios.common.descriptors.AgentInfo;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.master.MasterModel;
import java.util.List;
import java.util.stream.Collectors;
import org.joda.time.Instant;
import org.junit.Test;

public class DeadAgentReaperTest {

  private static final long TIMEOUT_HOURS = 1000;

  private static class Datapoint {

    private final String host;
    private final long startTime;
    private final long uptime;
    private final HostStatus.Status status;
    private final boolean expectReap;

    private Datapoint(final String host, final long startTimeHours, final long uptimeHours,
                      final HostStatus.Status status, final boolean expectReap) {
      this.host = host;
      this.startTime = HOURS.toMillis(startTimeHours);
      this.uptime = HOURS.toMillis(uptimeHours);
      this.status = status;
      this.expectReap = expectReap;
    }
  }

  @Test
  public void testDeadAgentReaper() throws Exception {
    final MasterModel masterModel = mock(MasterModel.class);
    final Clock clock = mock(Clock.class);
    when(clock.now()).thenReturn(new Instant(HOURS.toMillis(2000)));

    final List<Datapoint> datapoints = Lists.newArrayList(
        new Datapoint("host1", 0, TIMEOUT_HOURS - 1, HostStatus.Status.DOWN, true),
        new Datapoint("host2", 0, TIMEOUT_HOURS + 1, HostStatus.Status.DOWN, false),
        new Datapoint("host3", 1000, 1000, HostStatus.Status.UP, false),
        new Datapoint("host4", 500, 300, HostStatus.Status.DOWN, true),
        // Agents started in the future should not be reaped, even if they are reported as down
        new Datapoint("host5", 5000, 0, HostStatus.Status.DOWN, false),
        // Agents that are UP should not be reaped even if the start and uptime indicate that
        // they should
        new Datapoint("host6", 0, 0, HostStatus.Status.UP, false)
    );

    when(masterModel.listHosts()).thenReturn(Lists.newArrayList(
        datapoints.stream().map(input -> input.host).collect(Collectors.toList())));

    for (final Datapoint datapoint : datapoints) {
      when(masterModel.isHostUp(datapoint.host))
          .thenReturn(HostStatus.Status.UP == datapoint.status);
      when(masterModel.getAgentInfo(datapoint.host)).thenReturn(AgentInfo.newBuilder()
          .setStartTime(datapoint.startTime)
          .setUptime(datapoint.uptime)
          .build());
    }

    final DeadAgentReaper reaper = new DeadAgentReaper(masterModel, TIMEOUT_HOURS, clock, 100, 0);
    reaper.startAsync().awaitRunning();

    for (final Datapoint datapoint : datapoints) {
      if (datapoint.expectReap) {
        verify(masterModel, timeout(500)).deregisterHost(datapoint.host);
      } else {
        verify(masterModel, never()).deregisterHost(datapoint.host);
      }
    }
  }
}

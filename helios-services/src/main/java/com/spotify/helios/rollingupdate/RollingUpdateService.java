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

package com.spotify.helios.rollingupdate;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.spotify.helios.servicescommon.Reactor.Callback;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.master.HostMatcher;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.servicescommon.Reactor;
import com.spotify.helios.servicescommon.ReactorFactory;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deploys and undeploys jobs to implement the desired deployment group state.
 */
public class RollingUpdateService extends AbstractIdleService {

  private static final Logger log = LoggerFactory.getLogger(RollingUpdateService.class);

  private static final long UPDATE_INTERVAL = SECONDS.toMillis(1);
  private static final long HOST_UPDATE_INTERVAL = SECONDS.toMillis(1);

  private final MasterModel masterModel;
  private final Reactor hostUpdateReactor;
  private final Reactor rollingUpdateReactor;

  /**
   * Create a new RollingUpdateService.
   *
   * @param masterModel    The {@link MasterModel} to use for retrieving data.
   * @param reactorFactory The factory to use for creating reactors.
   */
  public RollingUpdateService(final MasterModel masterModel,
                              final ReactorFactory reactorFactory) {
    this.masterModel = checkNotNull(masterModel, "masterModel");
    checkNotNull(reactorFactory, "reactorFactory");

    this.hostUpdateReactor = reactorFactory.create("hostUpdate",
        new UpdateDeploymentGroupHosts(),
        HOST_UPDATE_INTERVAL);
    this.rollingUpdateReactor = reactorFactory.create("rollingUpdate", new RollingUpdate(),
        UPDATE_INTERVAL);
  }

  @Override
  protected void startUp() throws Exception {
    hostUpdateReactor.startAsync().awaitRunning();
    hostUpdateReactor.signal();

    rollingUpdateReactor.startAsync().awaitRunning();
    rollingUpdateReactor.signal();
  }

  @Override
  protected void shutDown() throws Exception {
    hostUpdateReactor.stopAsync().awaitTerminated();
    rollingUpdateReactor.stopAsync().awaitTerminated();
  }

  /**
   * Updates the list of hosts associated with a deployment group. Called by the hostUpdateReactor.
   */
  private class UpdateDeploymentGroupHosts implements Callback {

    @Override
    public void run(final boolean timeout) throws InterruptedException {
      final List<String> allHosts = masterModel.listHosts();
      final Map<String, Map<String, String>> hostsToLabels = Maps.newHashMap();

      // determine all hosts and their labels
      for (final String host : allHosts) {
        hostsToLabels.put(host, masterModel.getHostLabels(host));
      }

      final HostMatcher hostMatcher = new HostMatcher(hostsToLabels);

      for (final DeploymentGroup dg : masterModel.getDeploymentGroups().values()) {
        final List<String> matchingHosts = hostMatcher.getMatchingHosts(dg);

        try {
          masterModel.updateDeploymentGroupHosts(dg.getName(), matchingHosts);
        } catch (Exception e) {
          log.warn("error processing hosts update for deployment group: {} - {}", dg.getName(), e);
        }
      }
    }
  }

  /**
   * Processes rolling update tasks. Called by the rollingUpdateReactor.
   */
  private class RollingUpdate implements Callback {

    @Override
    public void run(final boolean timeout) throws InterruptedException {
      try {
        masterModel.rollingUpdateStep();
      } catch (Exception e) {
        log.error("error processing rolling update step: {}", e);
      }
    }
  }

}

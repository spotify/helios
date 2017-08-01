/*-
 * -\-\-
 * Helios Client
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

package com.spotify.helios.common.descriptors;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RolloutTask extends Descriptor {

  public enum Action {
    UNDEPLOY_OLD_JOBS,
    DEPLOY_NEW_JOB,
    AWAIT_RUNNING,
    FORCE_UNDEPLOY_JOBS,
    AWAIT_UNDEPLOYED,
    MARK_UNDEPLOYED,
  }

  public enum Status {
    OK,
    FAILED
  }

  private final Action action;
  private final String target;

  public static RolloutTask of(final Action action, final String target) {
    return new RolloutTask(action, target);
  }

  private RolloutTask(@JsonProperty("action") final Action action,
                      @JsonProperty("target") final String target) {
    this.action = checkNotNull(action, "action");
    this.target = checkNotNull(target, "target");
  }

  public Action getAction() {
    return action;
  }

  public String getTarget() {
    return target;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "RolloutTask{"
           + "action=" + action
           + ", target='" + target + '\''
           + '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    final RolloutTask that = (RolloutTask) obj;

    if (action != that.action) {
      return false;
    }
    return !(target != null ? !target.equals(that.target) : that.target != null);

  }

  @Override
  public int hashCode() {
    int result = action != null ? action.hashCode() : 0;
    result = 31 * result + (target != null ? target.hashCode() : 0);
    return result;
  }

  public static class Builder {
    private Action action;
    private String target;

    public Builder setAction(Action action) {
      this.action = action;
      return this;
    }

    public Builder setTarget(String target) {
      this.target = target;
      return this;
    }

    public RolloutTask build() {
      return new RolloutTask(action, target);
    }
  }
}

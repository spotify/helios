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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.Nullable;

/**
 * Basically, a pair of {@link JobId} and {@link Goal}.  This is different than {@link Task}
 * which has a {@link Job} and not a {@link JobId}.
 *
 * <p>A typical JSON representation might be:
 * <pre>
 * {
 *   "job": "myservice:0.5:3539b7bc2235d53f79e6e8511942bbeaa8816265",
 *   "goal": "START",
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Deployment extends Descriptor {

  public static final String EMTPY_DEPLOYER_USER = null;
  public static final String EMPTY_DEPLOYER_MASTER = null;
  public static final String EMPTY_DEPLOYMENT_GROUP_NAME = null;

  private final JobId jobId;
  private final Goal goal;
  private final String deployerUser;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String deployerMaster;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final String deploymentGroupName;

  /**
   * Constructor
   *
   * @param jobId               The id of the job.
   * @param goal                The desired state (i.e. goal) of the task/deployment.
   * @param deployerUser        The user doing the deployment.
   * @param deployerMaster      The master that created this deployment.
   * @param deploymentGroupName The deployment group this deployment is created by.
   */
  public Deployment(
      @JsonProperty("job") final JobId jobId,
      @JsonProperty("goal") final Goal goal,
      @JsonProperty("deployerUser") @Nullable final String deployerUser,
      @JsonProperty("deployerMaster") @Nullable final String deployerMaster,
      @JsonProperty("deploymentGroupName") @Nullable final String deploymentGroupName) {
    this.jobId = jobId;
    this.goal = goal;
    this.deployerUser = deployerUser;
    this.deploymentGroupName = deploymentGroupName;
    this.deployerMaster = deployerMaster;
  }

  public static Deployment of(final JobId jobId, final Goal goal) {
    return newBuilder()
        .setJobId(jobId)
        .setGoal(goal)
        .build();
  }

  public static Deployment of(final JobId jobId, final Goal goal, final String deployerUser) {
    return newBuilder()
        .setJobId(jobId)
        .setGoal(goal)
        .setDeployerUser(deployerUser)
        .build();
  }

  public static Deployment of(final JobId jobId, final Goal goal, final String deployerUser,
                              final String deployerMaster, final String deploymentGroupName) {
    return newBuilder()
        .setJobId(jobId)
        .setGoal(goal)
        .setDeployerUser(deployerUser)
        .setDeployerMaster(deployerMaster)
        .setDeploymentGroupName(deploymentGroupName)
        .build();
  }

  public JobId getJobId() {
    return jobId;
  }

  public Goal getGoal() {
    return goal;
  }

  public String getDeployerUser() {
    return deployerUser;
  }

  public String getDeploymentGroupName() {
    return deploymentGroupName;
  }

  public String getDeployerMaster() {
    return deployerMaster;
  }

  @Override
  public String toString() {
    return "Deployment{"
           + "jobId=" + jobId
           + ", goal=" + goal
           + ", deployerUser='" + deployerUser + '\''
           + ", deployerMaster='" + deployerMaster + '\''
           + ", deploymentGroupName='" + deploymentGroupName + '\''
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

    final Deployment that = (Deployment) obj;

    if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) {
      return false;
    }
    if (goal != that.goal) {
      return false;
    }
    if (deployerUser != null ? !deployerUser.equals(that.deployerUser)
                             : that.deployerUser != null) {
      return false;
    }
    if (deploymentGroupName != null ? !deploymentGroupName.equals(that.deploymentGroupName)
                                    : that.deploymentGroupName != null) {
      return false;
    }
    return !(deployerMaster != null ? !deployerMaster
        .equals(that.deployerMaster)
                                    : that.deployerMaster != null);

  }

  @Override
  public int hashCode() {
    int result = jobId != null ? jobId.hashCode() : 0;
    result = 31 * result + (goal != null ? goal.hashCode() : 0);
    result = 31 * result + (deployerUser != null ? deployerUser.hashCode() : 0);
    result = 31 * result + (deploymentGroupName != null ? deploymentGroupName.hashCode() : 0);
    result = 31 * result + (deployerMaster != null ? deployerMaster.hashCode() : 0);
    return result;
  }

  public Builder toBuilder() {
    return newBuilder()
        .setDeployerUser(deployerUser)
        .setGoal(goal)
        .setJobId(jobId);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private JobId jobId;
    private Goal goal;
    private String deployerUser;
    private String deployerMaster;
    private String deploymentGroupName;

    public Builder setJobId(final JobId jobId) {
      this.jobId = jobId;
      return this;
    }

    public Builder setGoal(final Goal goal) {
      this.goal = goal;
      return this;
    }

    public Builder setDeployerUser(final String deployerUser) {
      this.deployerUser = deployerUser;
      return this;
    }

    public Builder setDeploymentGroupName(String deploymentGroupName) {
      this.deploymentGroupName = deploymentGroupName;
      return this;
    }

    public Builder setDeployerMaster(String deployerMaster) {
      this.deployerMaster = deployerMaster;
      return this;
    }

    public Deployment build() {
      return new Deployment(jobId, goal, deployerUser, deployerMaster, deploymentGroupName);
    }
  }
}

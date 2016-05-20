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

package com.spotify.helios.master;

import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.RollingOperation;
import com.spotify.helios.common.descriptors.RollingOperationStatus;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.descriptors.TaskStatusEvent;

import java.util.List;
import java.util.Map;

/**
 * The interface that describes the kinds of information the Helios master needs from it's
 * configuration/coordination store.
 */
public interface MasterModel {

  void registerHost(String host, final String id);

  void deregisterHost(String host) throws HostNotFoundException, HostStillInUseException;

  List<String> listHosts();

  HostStatus getHostStatus(String host);

  void addJob(Job job) throws JobExistsException;

  Job getJob(JobId jobId);

  Map<JobId, Job> getJobs();

  JobStatus getJobStatus(JobId jobId);

  Job removeJob(JobId jobId)
      throws JobDoesNotExistException,
             JobStillDeployedException;

  Job removeJob(JobId jobId, String token)
      throws JobDoesNotExistException,
             JobStillDeployedException,
             TokenVerificationException;

  void deployJob(String host, Deployment job)
      throws HostNotFoundException,
             JobAlreadyDeployedException,
             JobDoesNotExistException,
             JobPortAllocationConflictException;

  void deployJob(String host, Deployment job, String token)
      throws HostNotFoundException,
             JobAlreadyDeployedException,
             JobDoesNotExistException,
             JobPortAllocationConflictException,
             TokenVerificationException;

  Deployment getDeployment(String host, JobId jobId);

  Deployment undeployJob(String host, JobId jobId)
      throws HostNotFoundException,
             JobNotDeployedException;

  Deployment undeployJob(String host, JobId jobId, String token)
      throws HostNotFoundException,
             JobNotDeployedException,
             TokenVerificationException;

  void updateDeployment(String host, Deployment deployment)
      throws HostNotFoundException,
             JobNotDeployedException;

  void updateDeployment(String host, Deployment deployment, String token)
      throws HostNotFoundException,
             JobNotDeployedException,
             TokenVerificationException;

  List<String> getRunningMasters();

  List<TaskStatusEvent> getJobHistory(JobId jobId) throws JobDoesNotExistException;

  List<TaskStatusEvent> getJobHistory(JobId jobId, String host) throws JobDoesNotExistException;

  void addDeploymentGroup(DeploymentGroup deploymentGroup) throws DeploymentGroupExistsException;

  DeploymentGroup getDeploymentGroup(String name) throws DeploymentGroupDoesNotExistException;

  Map<String, DeploymentGroup> getDeploymentGroups();

  List<String> getDeploymentGroupHosts(String name) throws DeploymentGroupDoesNotExistException;

  void updateDeploymentGroupHosts(String groupName, List<String> hosts)
      throws DeploymentGroupDoesNotExistException;

  DeploymentGroupStatus getDeploymentGroupStatus(String name)
      throws DeploymentGroupDoesNotExistException;

  List<RollingOperation> getRollingOperations(final String groupName)
      throws DeploymentGroupDoesNotExistException;

  RollingOperation getLastRollingOperation(final String groupName)
      throws DeploymentGroupDoesNotExistException;

  RollingOperationStatus getRollingOperationStatus(final String rollingOpId)
      throws RollingOperationDoesNotExistException;

  void removeDeploymentGroup(String name) throws DeploymentGroupDoesNotExistException;

  void rollingUpdate(DeploymentGroup deploymentGroup, JobId jobId, RolloutOptions options)
      throws DeploymentGroupDoesNotExistException, JobDoesNotExistException;

  void rollingUpdateStep();

  void stopDeploymentGroup(String deploymentGroupName) throws DeploymentGroupDoesNotExistException;
}

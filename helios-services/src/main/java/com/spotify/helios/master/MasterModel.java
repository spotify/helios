/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.TaskStatusEvent;

import java.util.List;
import java.util.Map;

public interface MasterModel {

  void registerHost(String host, final String id);

  void deregisterHost(String host) throws HostNotFoundException, HostStillInUseException;

  List<String> listHosts();

  HostStatus getHostStatus(String host);

  void addJob(Job job) throws JobExistsException;

  Job getJob(JobId job);

  Map<JobId, Job> getJobs();

  JobStatus getJobStatus(JobId jobId);

  Job removeJob(JobId job) throws JobDoesNotExistException,
                                  JobStillDeployedException;

  void deployJob(String host, Deployment job) throws HostNotFoundException,
                                                     JobAlreadyDeployedException,
                                                     JobDoesNotExistException,
                                                     JobPortAllocationConflictException;

  Deployment getDeployment(String host, JobId job);

  Deployment undeployJob(String host, JobId job) throws HostNotFoundException,
                                                        JobNotDeployedException;

  void updateDeployment(String host, Deployment deployment) throws HostNotFoundException,
                                                                   JobNotDeployedException;

  List<String> getRunningMasters();

  List<TaskStatusEvent> getJobHistory(JobId jobId) throws JobDoesNotExistException;
}

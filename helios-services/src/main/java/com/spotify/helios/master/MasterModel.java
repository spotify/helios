/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.spotify.helios.common.HostNotFoundException;
import com.spotify.helios.common.JobAlreadyDeployedException;
import com.spotify.helios.common.JobDoesNotExistException;
import com.spotify.helios.common.JobExistsException;
import com.spotify.helios.common.JobNotDeployedException;
import com.spotify.helios.common.JobPortAllocationConflictException;
import com.spotify.helios.common.JobStillDeployedException;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobStatus;
import com.spotify.helios.common.protocol.TaskStatusEvent;

import java.util.List;
import java.util.Map;

public interface MasterModel {

  void registerHost(String host);

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

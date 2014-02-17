/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.common.AgentDoesNotExistException;
import com.spotify.helios.common.JobAlreadyDeployedException;
import com.spotify.helios.common.JobDoesNotExistException;
import com.spotify.helios.common.JobExistsException;
import com.spotify.helios.common.JobNotDeployedException;
import com.spotify.helios.common.JobPortAllocationConflictException;
import com.spotify.helios.common.JobStillInUseException;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.JobStatus;
import com.spotify.helios.common.protocol.TaskStatusEvent;

import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;

public interface MasterModel {

  void addAgent(String agent);

  List<String> getAgents();

  void removeAgent(String agent) throws AgentDoesNotExistException;

  void addJob(Job job) throws JobExistsException;

  Job getJob(JobId job);

  Map<JobId, Job> getJobs();

  JobStatus getJobStatus(JobId jobId);

  Job removeJob(JobId job) throws JobDoesNotExistException,
                                  JobStillInUseException;

  void deployJob(String agent, Deployment job) throws AgentDoesNotExistException,
                                                      JobAlreadyDeployedException,
                                                      JobDoesNotExistException,
                                                      JobPortAllocationConflictException;

  Deployment getDeployment(String agent, JobId job);

  Deployment undeployJob(String agent, JobId job) throws AgentDoesNotExistException,
                                                         JobNotDeployedException;

  AgentStatus getAgentStatus(String agent);

  void updateDeployment(String agent, Deployment deployment) throws AgentDoesNotExistException,
                                                                    JobNotDeployedException;

  ImmutableList<String> getRunningMasters();

  List<TaskStatusEvent> getJobHistory(JobId jobId) throws JobDoesNotExistException;
}

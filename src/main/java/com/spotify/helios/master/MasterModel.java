/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.google.common.collect.ImmutableList;

import com.spotify.helios.common.HeliosException;
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

  // TODO (dano): instead of having all coordinator methods throwing checked HeliosExceptions on
  // internal failures we should throw runtime exceptions for situations like ZooKeeper being borked
  // and the caller cannot actually be expected to remedy the situation. Methods like getJobStatus
  // should probably declare and throw a JobDoesNotExistException when a pre-condition like the
  // existence of a job fails.

  void addAgent(String agent) throws HeliosException;

  List<String> getAgents() throws HeliosException;

  void removeAgent(String agent) throws HeliosException;

  void addJob(Job job) throws HeliosException;

  @Nullable
  Job getJob(JobId job) throws HeliosException;

  Map<JobId, Job> getJobs() throws HeliosException;

  JobStatus getJobStatus(JobId jobId) throws HeliosException;

  Job removeJob(JobId job) throws HeliosException;

  void deployJob(String agent, Deployment job) throws HeliosException;

  Deployment getDeployment(String agent, JobId job) throws HeliosException;

  Deployment undeployJob(String agent, JobId job) throws HeliosException;

  AgentStatus getAgentStatus(String agent) throws HeliosException;

  void updateDeployment(String agent, Deployment deployment) throws HeliosException;

  ImmutableList<String> getRunningMasters() throws HeliosException;

  List<TaskStatusEvent> getJobHistory(JobId jobId) throws HeliosException;
}

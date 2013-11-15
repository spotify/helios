/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.master;

import com.google.common.collect.ImmutableList;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.JobExistsException;
import com.spotify.helios.common.descriptors.AgentJob;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.JobDescriptor;

import java.util.List;
import java.util.Map;

public interface Coordinator {

  void addAgent(String agent) throws HeliosException;

  List<String> getAgents() throws HeliosException;

  void removeAgent(String agent) throws HeliosException;

  void addJob(JobDescriptor job) throws JobExistsException, HeliosException;

  JobDescriptor getJob(String job) throws HeliosException;

  Map<String, JobDescriptor> getJobs() throws HeliosException;

  JobDescriptor removeJob(String job) throws HeliosException;

  void addAgentJob(String agent, AgentJob job) throws HeliosException;

  AgentJob getAgentJob(String agent, String job) throws HeliosException;

  AgentJob removeAgentJob(String agent, String job) throws HeliosException;

  AgentStatus getAgentStatus(String agent) throws HeliosException;

  void updateAgentJob(String agent, AgentJob agentJob) throws HeliosException;

  ImmutableList<String> getRunningMasters() throws HeliosException;
}

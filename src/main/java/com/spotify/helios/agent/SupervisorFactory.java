package com.spotify.helios.agent;

import com.spotify.helios.common.coordination.DockerClientFactory;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates job supervisors.
 *
 * @see Supervisor
 */
public class SupervisorFactory {

  private final AgentModel model;
  private final DockerClientFactory dockerClientFactory;
  private final String[] envVars;

  public SupervisorFactory(final AgentModel model, final DockerClientFactory dockerClientFactory,
                           final String[] envVars) {
    this.dockerClientFactory = dockerClientFactory;
    this.model = checkNotNull(model);
    this.envVars = checkNotNull(envVars);
  }

  /**
   * Create a new application container.
   *
   * @return A new container.
   */
  public Supervisor create(final JobId jobId, final Job descriptor) {
    final AsyncDockerClient dockerClient = new AsyncDockerClient(dockerClientFactory);
    return Supervisor.newBuilder()
        .setJobId(jobId)
        .setDescriptor(descriptor)
        .setModel(model)
        .setDockerClient(dockerClient)
        .setEnvVars(envVars)
        .build();
  }
}

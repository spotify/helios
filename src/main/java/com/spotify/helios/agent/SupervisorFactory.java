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

  private final State state;
  private final DockerClientFactory dockerClientFactory;
  private final AgentConfig config;

  public SupervisorFactory(final State state, final DockerClientFactory dockerClientFactory,
                           final AgentConfig config) {
    this.dockerClientFactory = dockerClientFactory;
    this.state = checkNotNull(state);
    this.config = checkNotNull(config);
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
        .setState(state)
        .setDockerClient(dockerClient)
        .setConfig(config)
        .build();
  }
}

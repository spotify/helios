package com.spotify.helios.agent;

import com.spotify.helios.common.coordination.DockerClientFactory;
import com.spotify.helios.common.descriptors.JobDescriptor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates job supervisors.
 *
 * @see Supervisor
 */
public class SupervisorFactory {

  private final State state;
  private final DockerClientFactory dockerClientFactory;

  public SupervisorFactory(final State state, final DockerClientFactory dockerClientFactory) {
    this.dockerClientFactory = dockerClientFactory;
    this.state = checkNotNull(state);
  }

  /**
   * Create a new application container.
   *
   * @return A new container.
   */
  public Supervisor create(final String name, final JobDescriptor descriptor) {
    final AsyncDockerClient dockerClient = new AsyncDockerClient(dockerClientFactory);
    return Supervisor.newBuilder()
        .setName(name)
        .setDescriptor(descriptor)
        .setState(state)
        .setDockerClient(dockerClient)
        .build();
  }
}

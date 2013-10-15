package com.spotify.helios.service.coordination;

import com.spotify.helios.service.descriptors.JobDescriptor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates application containers.
 *
 * @see JobRunner
 */
public class JobRunnerFactory {

  private final State state;
  private final DockerClientFactory dockerClientFactory;

  public JobRunnerFactory(final State state, final DockerClientFactory dockerClientFactory) {
    this.dockerClientFactory = dockerClientFactory;
    this.state = checkNotNull(state);
  }

  /**
   * Create a new application container.
   *
   * @return A new container.
   */
  public JobRunner create(final String name, final JobDescriptor descriptor) {
    return new JobRunner(name, descriptor, state, dockerClientFactory);
  }
}

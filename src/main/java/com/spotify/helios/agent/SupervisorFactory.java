package com.spotify.helios.agent;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.nameless.client.NamelessRegistrar;

import java.util.Map;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Creates job supervisors.
 *
 * @see Supervisor
 */
public class SupervisorFactory {

  private final AgentModel model;
  private final DockerClientFactory dockerClientFactory;
  private final Map<String, String> envVars;
  private final NamelessRegistrar registrar;

  public SupervisorFactory(final AgentModel model, final DockerClientFactory dockerClientFactory,
                           final Map<String, String> envVars,
                           final @Nullable NamelessRegistrar registrar) {
    this.dockerClientFactory = dockerClientFactory;
    this.model = checkNotNull(model);
    this.envVars = checkNotNull(envVars);
    this.registrar = registrar;
  }

  /**
   * Create a new application container.
   *
   * @return A new container.
   */
  public Supervisor create(final JobId jobId, final Job descriptor) {
    final RestartPolicy policy = RestartPolicy.newBuilder().build();
    final TaskStatusManagerImpl manager = TaskStatusManagerImpl.newBuilder()
        .setJobId(jobId)
        .setJob(descriptor)
        .setModel(model)
        .build();
    final FlapController flapController = FlapController.newBuilder()
        .setJobId(jobId)
        .setTaskStatusManager(manager)
        .build();
    final AsyncDockerClient dockerClient = new AsyncDockerClient(dockerClientFactory);
    return Supervisor.newBuilder()
        .setJobId(jobId)
        .setDescriptor(descriptor)
        .setModel(model)
        .setDockerClient(dockerClient)
        .setEnvVars(envVars)
        .setFlapController(flapController)
        .setRestartPolicy(policy)
        .setTaskStatusManager(manager)
        .setNamelessRegistrar(registrar)
        .build();
  }
}

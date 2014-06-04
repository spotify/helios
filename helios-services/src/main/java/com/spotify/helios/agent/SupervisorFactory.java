package com.spotify.helios.agent;

import com.spotify.docker.DockerClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

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
  private final DockerClient dockerClient;
  private final Map<String, String> envVars;
  private final ServiceRegistrar registrar;
  private final ContainerDecorator containerDecorator;
  private final String host;
  private final SupervisorMetrics metrics;
  private final RiemannFacade riemannFacade;

  public SupervisorFactory(final AgentModel model, final DockerClient dockerClient,
                           final Map<String, String> envVars,
                           final @Nullable ServiceRegistrar registrar,
                           final ContainerDecorator containerDecorator,
                           final String host,
                           final SupervisorMetrics supervisorMetrics,
                           final RiemannFacade riemannFacade) {
    this.dockerClient = dockerClient;
    this.model = checkNotNull(model);
    this.envVars = checkNotNull(envVars);
    this.registrar = registrar;
    this.containerDecorator = containerDecorator;
    this.host = host;
    this.metrics = supervisorMetrics;
    this.riemannFacade = riemannFacade;
  }

  /**
   * Create a new application container.
   *
   * @return A new container.
   */
  public Supervisor create(final JobId jobId, final Job descriptor,
                           final Map<String, Integer> ports, final Supervisor.Listener listener) {
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
    return Supervisor.newBuilder()
        .setHost(host)
        .setJobId(jobId)
        .setJob(descriptor)
        .setModel(model)
        .setDockerClient(dockerClient)
        .setEnvVars(envVars)
        .setFlapController(flapController)
        .setRestartPolicy(policy)
        .setTaskStatusManager(manager)
        .setServiceRegistrar(registrar)
        .setContainerDecorator(containerDecorator)
        .setMetrics(metrics)
        .setPorts(ports)
        .setListener(listener)
        .build();
  }
}

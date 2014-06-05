package com.spotify.helios.agent;

import com.google.common.base.Supplier;

import com.spotify.docker.DockerClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.serviceregistration.ServiceRegistrar;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;

public class TaskRunnerFactory {
  private final Job job;
  private final ContainerUtil containerUtil;
  private final SupervisorMetrics metrics;
  private final DockerClient docker;
  private final FlapController flapController;
  private final ServiceRegistrar registrar;

  public TaskRunnerFactory(final ServiceRegistrar registrar,
                           final Job job,
                           final ContainerUtil containerUtil,
                           final SupervisorMetrics metrics,
                           final DockerClient docker,
                           final FlapController flapController) {
    this.registrar = registrar;
    this.job = job;
    this.containerUtil = containerUtil;
    this.metrics = metrics;
    this.docker = docker;
    this.flapController = checkNotNull(flapController);
  }

  public TaskRunner create(final long delay,
                           final Supplier<String> containerIdSupplier,
                           final AtomicReference<ThrottleState> throttle,
                           final StatusUpdater statusUpdater) {
    return new TaskRunner(delay, registrar, job, containerUtil, metrics, docker, flapController,
                          throttle, statusUpdater, containerIdSupplier);
  }
}

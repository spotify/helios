package com.spotify.helios.agent;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.Job.Builder;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.serviceregistration.ServiceRegistration.Endpoint;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TaskConfigTest {

  private static final String IP_PROTO = "tcp";
  private static final String APP_PROTO = "http";
  private static final String SERVICE_NAME = "MyService";
  private static final String PORT_NAME = "MyPort";
  private static final Integer EXTERNAL_PORT = 20000;
  private static final Integer INTERNAL_PORT = 2929;
  private static final Map<String, List<PortBinding>> IGNORE_DOCKER_PORTS = ImmutableMap.of();

  @Test
  public void testRegistration_dynamicPort() throws Exception {
    final Builder jobBuilder = makeJobBuilder();
    jobBuilder.setPorts(ImmutableMap.of(PORT_NAME, PortMapping.of(INTERNAL_PORT)));
    final TaskConfig tc = makeTaskConfig(jobBuilder);

    final Map<String, List<PortBinding>> dockerPorts = ImmutableMap.of(
        INTERNAL_PORT + "/" + IP_PROTO,
        (List<PortBinding>) ImmutableList.of(PortBinding.of("0.0.0.0", EXTERNAL_PORT)));

    final List<Endpoint> endPoints = tc.registration(dockerPorts).getEndpoints();
    assertEquals(1, endPoints.size());
    final Endpoint endPoint = endPoints.get(0);

    assertEquals(EXTERNAL_PORT, (Integer) endPoint.getPort());
    assertEquals(SERVICE_NAME, endPoint.getName());
  }

  @Test
  public void testRegistration_noPort() throws Exception {
    final Builder jobBuilder = makeJobBuilder();
    jobBuilder.setPorts(ImmutableMap.of(PORT_NAME, PortMapping.of(INTERNAL_PORT)));
    final TaskConfig tc = makeTaskConfig(jobBuilder);

    final List<Endpoint> endPoints = tc.registration(IGNORE_DOCKER_PORTS).getEndpoints();
    assertEquals(0, endPoints.size());
  }

  @Test
  public void testRegistration_explicitPort() throws Exception {
    final Builder jobBuilder = makeJobBuilder();
    jobBuilder.setPorts(ImmutableMap.of(PORT_NAME, PortMapping.of(INTERNAL_PORT, EXTERNAL_PORT)));
    final TaskConfig tc = makeTaskConfig(jobBuilder);

    final List<Endpoint> endPoints = tc.registration(IGNORE_DOCKER_PORTS).getEndpoints();
    assertEquals(1, endPoints.size());
    final Endpoint endPoint = endPoints.get(0);

    assertEquals(EXTERNAL_PORT, (Integer) endPoint.getPort());
    assertEquals(SERVICE_NAME, endPoint.getName());
  }

  private TaskConfig makeTaskConfig(Builder jobBuilder) {
    final Map<String, Integer> ports = ImmutableMap.of(PORT_NAME, INTERNAL_PORT);

    final TaskConfig tc = TaskConfig.builder()
        .containerDecorator(new NoOpContainerDecorator())
        .defaultRegistrationDomain("")
        .envVars(Maps.<String, String>newHashMap())
        .host("HOST")
        .job(jobBuilder.build())
        .namespace("NAMESPACE")
        .ports(ports)
        .build();
    return tc;
  }

  private Builder makeJobBuilder() {
    final Builder job = Job.newBuilder()
        .setName("NAME")
        .setImage("IMAGE")
        .setVersion("VERSION")
        .setRegistration(ImmutableMap.of(
            ServiceEndpoint.of(SERVICE_NAME, APP_PROTO),
            ServicePorts.of(PORT_NAME)))
        .addPort(PORT_NAME, PortMapping.of(2929, IP_PROTO));
    return job;
  }
}

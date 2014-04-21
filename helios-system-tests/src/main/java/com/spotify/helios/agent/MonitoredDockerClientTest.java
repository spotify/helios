package com.spotify.helios.agent;

import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.model.ContainerConfig;
import com.kpelykh.docker.client.model.HostConfig;
import com.spotify.helios.TemporaryPorts;
import com.spotify.helios.servicescommon.NoOpRiemannClient;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.spotify.helios.servicescommon.statistics.NoopSupervisorMetrics;
import com.spotify.helios.servicescommon.statistics.SupervisorMetrics;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MonitoredDockerClientTest {

  @Rule public final TemporaryPorts temporaryPorts = new TemporaryPorts();

  private int badDockerPort = temporaryPorts.localPort("bad-docker");

  private BadDockerServer badDocker;

  private MonitoredDockerClient client;

  @Before
  public void setUp() throws Exception {
    badDocker = new BadDockerServer(badDockerPort);
    badDocker.start();
    DockerClientFactory factory = new DockerClientFactory("http://localhost:" + badDockerPort);
    AsyncDockerClient asClient = new AsyncDockerClient(factory);
    SupervisorMetrics metrics = new NoopSupervisorMetrics();
    RiemannFacade riemannFacade = new NoOpRiemannClient().facade();
    client = new MonitoredDockerClient(asClient, metrics, riemannFacade,
        1, 3, 6);
  }

  @After
  public void tearDown() throws Exception {
    badDocker.stop();
  }

  @Test
  public void testInspectContainer() throws Exception {
    try {
      client.inspectContainer("doesnotmatter");
      fail("NOOOO");
    } catch (DockerException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }

  @Test
  public void testInspectImage() throws Exception {
    try {
      client.inspectImage("doesnotmatter");
      fail("NOOOO");
    } catch (DockerException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }

  @Test
  public void testCreateContainer() throws Exception {
    try {
      client.createContainer(new ContainerConfig(), "dunnamatter");
      fail("NOOOO");
    } catch (DockerException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }

  @Test
  public void testPull() throws Exception {
    try {
      client.pull("name");
      fail("NOOOO");
    } catch (DockerException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }

  @Test
  public void testKill() throws Exception {
    try {
      client.kill("xxx");
    } catch (DockerException e) {
      assertEquals(TimeoutException.class, e.getCause().getClass());
    }
  }
}

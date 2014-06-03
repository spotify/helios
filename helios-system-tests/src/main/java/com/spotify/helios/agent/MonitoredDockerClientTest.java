package com.spotify.helios.agent;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.Promise;
import com.spotify.helios.agent.docker.DockerClient;
import com.spotify.helios.agent.docker.DockerException;
import com.spotify.helios.agent.docker.DockerTimeoutException;
import com.spotify.helios.servicescommon.RiemannFacade;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MonitoredDockerClientTest {

  private static final String HOST = "FOO_HOST";
  private static final String SERVICE = "FOO_SERVICE";

  @Rule public ExpectedException exception = ExpectedException.none();
  @Mock public DockerClient client;
  @Mock public AbstractRiemannClient riemannClient;

  @Captor public ArgumentCaptor<Proto.Event> eventCaptor;

  private DockerClient sut;

  @Before
  public void setUp() throws Exception {
    when(riemannClient.aSendEventsWithAck(eventCaptor.capture()))
        .thenReturn(new Promise<Boolean>());
    when(riemannClient.event()).thenReturn(new EventDSL(riemannClient));
    final RiemannFacade riemannFacade = new RiemannFacade(riemannClient, HOST, SERVICE);
    sut = MonitoredDockerClient.wrap(riemannFacade, client);
  }

  @Test()
  public void testRequestTimeout() throws Exception {
    when(client.inspectContainer(anyString())).thenThrow(mock(DockerTimeoutException.class));
    try {
      sut.inspectContainer("foo");
      fail();
    } catch (DockerTimeoutException ignore) {
    }
    final Proto.Event event = eventCaptor.getValue();
    assertThat(event.getTagsList(), contains("docker", "timeout", "inspectContainer"));
    assertThat(event.getService(), equalTo("helios-agent/docker"));
  }

  @Test()
  public void testRequestError() throws Exception {
    when(client.inspectImage(anyString())).thenThrow(mock(DockerException.class));
    try {
      sut.inspectImage("bar");
      fail();
    } catch (DockerException ignore) {
    }
    final Proto.Event event = eventCaptor.getValue();
    assertThat(event.getTagsList(), contains("docker", "error", "inspectImage"));
    assertThat(event.getService(), equalTo("helios-agent/docker"));
  }
}

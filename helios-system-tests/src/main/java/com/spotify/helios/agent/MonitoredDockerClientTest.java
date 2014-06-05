/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.agent;

import com.aphyr.riemann.Proto;
import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.Promise;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.DockerTimeoutException;
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

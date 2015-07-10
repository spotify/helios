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

package com.spotify.helios.cli.command;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.protocol.CreateDeploymentGroupResponse;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.Namespace;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeploymentGroupCreateCommandTest {

  private static final String GROUP_NAME = "foo-group";
  private static final Map<String, String> LABELS_MAP = ImmutableMap.of("foo", "bar", "baz", "qux");
  private static final List<String> LABELS = ImmutableList.of("foo=bar", "baz=qux");

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);
  private final DeploymentGroupCreateCommand command = new DeploymentGroupCreateCommand(
      ArgumentParsers.newArgumentParser("test").addSubparsers()
          .addParser("create-deployment-group"));


  private void mockCreateResponse(final CreateDeploymentGroupResponse.Status status) {
    when(client.createDeploymentGroup(any(DeploymentGroup.class)))
        .thenReturn(Futures.immediateFuture(new CreateDeploymentGroupResponse(status)));

  }

  @Before
  public void setUp() {
    // Set up default CLI arguments and options to reduce test boilerplate
    when(options.getString("name")).thenReturn(GROUP_NAME);
    doReturn(ImmutableList.of(LABELS)).when(options).getList("labels");
  }

  @Test
  public void testCreateDeploymentGroup() throws Exception {
    mockCreateResponse(CreateDeploymentGroupResponse.Status.CREATED);

    assertEquals(0, command.run(options, client, out, false, null));

    final ArgumentCaptor<DeploymentGroup> captor = ArgumentCaptor.forClass(DeploymentGroup.class);
    verify(client).createDeploymentGroup(captor.capture());
    assertEquals(GROUP_NAME, captor.getValue().getName());
    assertEquals(LABELS_MAP, captor.getValue().getLabels());

    final String output = baos.toString();
    assertThat(output, containsString("\"name\":\"foo-group\""));
    assertThat(output, containsString("\"labels\":{\"baz\":\"qux\",\"foo\":\"bar\"}"));
  }

  @Test
  public void testCreateAlreadyExistingDeploymentGroup() throws Exception {
    mockCreateResponse(CreateDeploymentGroupResponse.Status.CREATED);
    assertEquals(0, command.run(options, client, out, false, null));

    mockCreateResponse(CreateDeploymentGroupResponse.Status.NOT_MODIFIED);
    assertEquals(0, command.run(options, client, out, false, null));

    final String output = baos.toString();
    assertThat(output, containsString("\"name\":\"foo-group\""));
    assertThat(output, containsString("\"labels\":{\"baz\":\"qux\",\"foo\":\"bar\"}"));
  }

  @Test
  public void testCreateConflictingDeploymentGroup() throws Exception {
    mockCreateResponse(CreateDeploymentGroupResponse.Status.CREATED);
    assertEquals(0, command.run(options, client, out, false, null));

    mockCreateResponse(CreateDeploymentGroupResponse.Status.CONFLICT);
    assertEquals(1, command.run(options, client, out, false, null));

    final String output = baos.toString();
    assertThat(output, containsString("\"name\":\"foo-group\""));
    assertThat(output, containsString("\"labels\":{\"baz\":\"qux\",\"foo\":\"bar\"}"));
    assertThat(output, containsString("CONFLICT"));
  }
}
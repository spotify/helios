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
import com.google.common.util.concurrent.Futures;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.protocol.CreateDeploymentGroupResponse;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatcher;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DeploymentGroupCreateCommandTest {

  private static final String NAME = "foo-group";
  private static final List<String> LABELS_EL = ImmutableList.of("foo=bar", "baz=qux");
  private static final List<List<String>> LABELS = ImmutableList.of(LABELS_EL);

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);

  private DeploymentGroupCreateCommand command;

  final CreateDeploymentGroupResponse okResponse =
      new CreateDeploymentGroupResponse(CreateDeploymentGroupResponse.Status.OK,
                                        Collections.<String>emptyList(), "12345");

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Before
  public void setUp() {
    // use a real, dummy Subparser impl to avoid having to mock out every single call
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("test");
    final Subparser subparser = parser.addSubparsers().addParser("create");

    command = new DeploymentGroupCreateCommand(subparser);

    when(client.createDeploymentGroup(deploymentGroupWhoseNameIs(NAME)))
        .thenReturn(Futures.immediateFuture(okResponse));
  }

  @Test
  public void testValidDeploymentGroupCreateCommand() throws Exception {
    when(options.getString("name")).thenReturn(NAME);
    doReturn(LABELS).when(options).getList("labels");
    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);
    final String output = baos.toString();
    assertThat(output, containsString("\"name\":\"foo-group\""));
    assertThat(output, containsString("\"labels\":{\"baz\":\"qux\",\"foo\":\"bar\"}"));
    assertThat(output, containsString("\"job\":\"\""));
  }

  @Test
  public void testInalidDeploymentGroupCreateCommand() throws Exception {
    expectedEx.expect(IllegalArgumentException.class);
    expectedEx.expectMessage("Please specify a name and at least one label.");

    when(options.getString("name")).thenReturn(NAME);
    doReturn(emptyMap()).when(options).get("labels");
    command.run(options, client, out, false, null);
  }

  private static DeploymentGroup deploymentGroupWhoseNameIs(final String name) {
    return argThat(new ArgumentMatcher<DeploymentGroup>() {
      @Override
      public boolean matches(Object argument) {
        if (argument instanceof DeploymentGroup) {
          final DeploymentGroup deploymentGroup = (DeploymentGroup) argument;
          if (deploymentGroup.getName().equals(name)) {
            return true;
          }
        }
        return false;
      }
    });
  }

}
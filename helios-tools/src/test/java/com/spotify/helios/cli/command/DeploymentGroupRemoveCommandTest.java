/*-
 * -\-\-
 * Helios Tools
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.cli.command;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.spotify.helios.common.protocol.RemoveDeploymentGroupResponse.Status;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.protocol.RemoveDeploymentGroupResponse;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.Before;
import org.junit.Test;

public class DeploymentGroupRemoveCommandTest {

  private static final String GROUP_NAME = "my_group";

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);
  private final DeploymentGroupRemoveCommand command = new DeploymentGroupRemoveCommand(
      ArgumentParsers.newArgumentParser("test").addSubparsers()
          .addParser("remove-deployment-group"));

  @Before
  public void before() {
    // Default CLI argument stubs
    when(options.getString("deployment-group-name")).thenReturn(GROUP_NAME);
  }

  @Test
  public void testRemoveDeploymentGroupRemoved() throws Exception {
    when(client.removeDeploymentGroup(GROUP_NAME))
        .thenReturn(immediateFuture(new RemoveDeploymentGroupResponse(Status.REMOVED)));

    final int ret = command.run(options, client, out, false, null);
    final String output = baos.toString();

    verify(client).removeDeploymentGroup(GROUP_NAME);
    assertEquals(0, ret);

    final String expected = format("Deployment-group %s removed\n", GROUP_NAME);

    assertEquals(expected, output);
  }

  @Test
  public void testRemoveDeploymentGroupRemovedJson() throws Exception {
    when(client.removeDeploymentGroup(GROUP_NAME))
        .thenReturn(immediateFuture(new RemoveDeploymentGroupResponse(Status.REMOVED)));

    final int ret = command.run(options, client, out, true, null);
    final String output = baos.toString();

    verify(client).removeDeploymentGroup(GROUP_NAME);
    assertEquals(0, ret);

    final RemoveDeploymentGroupResponse res = Json.read(
        output, new TypeReference<RemoveDeploymentGroupResponse>() {});

    assertEquals(new RemoveDeploymentGroupResponse(Status.REMOVED), res);
  }

  @Test
  public void testRemoveDeploymentGroupNotFound() throws Exception {
    when(client.removeDeploymentGroup(GROUP_NAME))
        .thenReturn(immediateFuture(
            new RemoveDeploymentGroupResponse(Status.DEPLOYMENT_GROUP_NOT_FOUND)));

    final int ret = command.run(options, client, out, false, null);
    final String output = baos.toString();

    verify(client).removeDeploymentGroup(GROUP_NAME);
    assertEquals(1, ret);

    final String expected = format("Failed to remove deployment-group %s, status: %s\n", GROUP_NAME,
        Status.DEPLOYMENT_GROUP_NOT_FOUND);

    assertEquals(expected, output);
  }

  @Test
  public void testRemoveDeploymentGroupNotFoundJson() throws Exception {
    when(client.removeDeploymentGroup(GROUP_NAME))
        .thenReturn(immediateFuture(
            new RemoveDeploymentGroupResponse(Status.DEPLOYMENT_GROUP_NOT_FOUND)));

    final int ret = command.run(options, client, out, true, null);
    final String output = baos.toString();

    verify(client).removeDeploymentGroup(GROUP_NAME);
    assertEquals(1, ret);

    final RemoveDeploymentGroupResponse res = Json.read(
        output, new TypeReference<RemoveDeploymentGroupResponse>() {});

    assertEquals(new RemoveDeploymentGroupResponse(Status.DEPLOYMENT_GROUP_NOT_FOUND), res);
  }
}

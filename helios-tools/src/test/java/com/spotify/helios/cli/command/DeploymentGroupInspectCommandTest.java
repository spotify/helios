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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.descriptors.JobId;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DeploymentGroupInspectCommandTest {

  private static final String NAME = "foo-group";
  private static final String NON_EXISTENT_NAME = "bar-group";
  private static final JobId JOB = new JobId("foo-job", "0.1.0");
  private static final List<HostSelector> HOST_SELECTORS = ImmutableList.of(
      HostSelector.parse("foo=bar"),
      HostSelector.parse("baz=qux"));
  private static final DeploymentGroup DEPLOYMENT_GROUP = DeploymentGroup.newBuilder()
      .setName(NAME).setHostSelectors(HOST_SELECTORS).setJobId(JOB).build();

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);

  private DeploymentGroupInspectCommand command;

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Before
  public void setUp() {
    // use a real, dummy Subparser impl to avoid having to mock out every single call
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("test");
    final Subparser subparser = parser.addSubparsers().addParser("inspect");

    command = new DeploymentGroupInspectCommand(subparser);

    when(client.deploymentGroup(NAME)).thenReturn(Futures.immediateFuture(DEPLOYMENT_GROUP));
    final ListenableFuture<DeploymentGroup> nullFuture = Futures.immediateFuture(null);
    when(client.deploymentGroup(NON_EXISTENT_NAME)).thenReturn(nullFuture);
  }

  @Test
  public void testDeploymentGroupInspectCommand() throws Exception {
    when(options.getString("name")).thenReturn(NAME);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);
    final String output = baos.toString();
    assertThat(output, containsString("Name: " + NAME));
    assertThat(output, containsString("Host selectors:"));
    assertThat(output, containsString("  foo = bar"));
    assertThat(output, containsString("  baz = qux"));
    assertThat(output, containsString("Job: " + JOB.toString()));
  }

  @Test
  public void testDeploymentGroupInspectCommandJson() throws Exception {
    when(options.getString("name")).thenReturn(NAME);
    final int ret = command.run(options, client, out, true, null);

    assertEquals(0, ret);
    final DeploymentGroup output = Json.read(baos.toString(), DeploymentGroup.class);

    assertEquals(DEPLOYMENT_GROUP, output);
  }

  @Test
  public void testDeploymentGroupInspectCommandNotFound() throws Exception {
    when(options.getString("name")).thenReturn(NON_EXISTENT_NAME);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(1, ret);
    final String output = baos.toString();
    assertThat(output, containsString("Unknown deployment group: " + NON_EXISTENT_NAME));
  }

  @Test
  public void testDeploymentGroupInspectCommandNotFoundJson() throws Exception {
    when(options.getString("name")).thenReturn(NON_EXISTENT_NAME);
    final int ret = command.run(options, client, out, true, null);

    assertEquals(1, ret);
    final Map<String, Object> output = Json.read(
        baos.toString(), new TypeReference<Map<String, Object>>() {});

    assertEquals("DEPLOYMENT_GROUP_NOT_FOUND", output.get("status"));
  }
}

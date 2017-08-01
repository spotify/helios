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
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.protocol.CreateJobResponse;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.hamcrest.CustomTypeSafeMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobCreateCommandTest {

  private static final Logger log = LoggerFactory.getLogger(JobCreateCommandTest.class);

  private static final String JOB_NAME = "foo";
  private static final String JOB_ID = JOB_NAME + ":123";
  private static final String EXEC_HEALTH_CHECK = "touch /this";
  private static final List<String> SECURITY_OPT =
      ImmutableList.of("label:user:dxia", "apparmor:foo");
  private static final String NETWORK_MODE = "host";

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);

  private JobCreateCommand command;

  private Map<String, String> envVars = Maps.newHashMap();

  @Before
  public void setUp() {
    // use a real, dummy Subparser impl to avoid having to mock out every single call
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("test");
    final Subparser subparser = parser.addSubparsers().addParser("create");

    final Supplier<Map<String, String>> envVarSupplier = new Supplier<Map<String, String>>() {
      @Override
      public Map<String, String> get() {
        return ImmutableMap.copyOf(envVars);
      }
    };

    command = new JobCreateCommand(subparser, envVarSupplier);

    when(client.createJob(argThat(matchesName(JOB_NAME)))).thenReturn(immediateFuture(
        new CreateJobResponse(CreateJobResponse.Status.OK,
            Collections.<String>emptyList(),
            "12345")
    ));
  }

  private int runCommand() throws InterruptedException, ExecutionException, IOException {
    return runCommand(false);
  }

  private int runCommand(boolean json)
      throws InterruptedException, ExecutionException, IOException {
    final int ret = command.run(options, client, out, json, null);
    log.debug("Output from command: [{}]", baos.toString().replaceAll("\n", "\\\\n"));
    return ret;
  }

  @Test
  public void testValidJobCreateCommand() throws Exception {
    when(options.getString("id")).thenReturn(JOB_ID);
    when(options.getString("image")).thenReturn("spotify/busybox:latest");
    when(options.getString("exec_check")).thenReturn(EXEC_HEALTH_CHECK);
    // For some reason the mocked options.getInt() returns 0 by default.
    // Explicitly return null to check that the value from the JSON file doesn't get overwritten.
    when(options.getInt("grace_period")).thenReturn(null);
    // TODO (mbrown): this path is weird when running from IntelliJ, should be changed to not
    // care about CWD
    doReturn(new File("src/test/resources/job_config.json")).when(options).get("file");
    doReturn(SECURITY_OPT).when(options).getList("security_opt");
    when(options.getString("network_mode")).thenReturn(NETWORK_MODE);
    when(options.getList("metadata")).thenReturn(Lists.<Object>newArrayList("a=1", "b=2"));
    when(options.getList("labels")).thenReturn(Lists.<Object>newArrayList("a=b", "c=d"));
    doReturn(ImmutableList.of("cap1", "cap2")).when(options).getList("add_capability");
    doReturn(ImmutableList.of("cap3", "cap4")).when(options).getList("drop_capability");

    final int ret = runCommand();

    assertEquals(0, ret);
    final String output = baos.toString();
    assertThat(output, containsString("\"created\":null"));
    assertThat(output, containsString(
        "\"env\":{\"JVM_ARGS\":\"-Ddw.feature.randomFeatureFlagEnabled=true\"}"));
    assertThat(output, containsString("\"metadata\":{\"a\":\"1\",\"b\":\"2\"},"));
    assertThat(output, containsString("\"gracePeriod\":100"));
    assertThat(output, containsString(
        "\"healthCheck\":{\"type\":\"exec\","
        + "\"command\":[\"touch\",\"/this\"],\"type\":\"exec\"},"));
    assertThat(output, containsString("\"securityOpt\":[\"label:user:dxia\",\"apparmor:foo\"]"));
    assertThat(output, containsString("\"networkMode\":\"host\""));
    assertThat(output, containsString("\"expires\":null"));
    assertThat(output, containsString("\"addCapabilities\":[\"cap1\",\"cap2\"]"));
    assertThat(output, containsString("\"dropCapabilities\":[\"cap3\",\"cap4\"]"));
    assertThat(output, containsString("\"labels\":{\"a\":\"b\",\"c\":\"d\"}"));
  }

  @Test
  public void testJobCreateCommandFailsWithInvalidJobId() throws Exception {
    when(options.getString("id")).thenReturn(JOB_NAME);
    when(options.getString("image")).thenReturn("spotify/busybox:latest");
    final int ret = runCommand();
    assertEquals(1, ret);
  }

  @Test
  public void testJobCreateCommandFailsWithInvalidPortProtocol() throws Exception {
    when(options.getString("id")).thenReturn(JOB_ID);
    when(options.getString("image")).thenReturn("spotify/busybox:latest");
    doReturn(ImmutableList.of("dns=53:53/http")).when(options).getList("port");
    final int ret = runCommand(true);

    assertEquals(1, ret);
    final String output = baos.toString();
    assertThat(output, containsString("\"status\":\"INVALID_JOB_DEFINITION\"}"));
    assertThat(output, containsString("\"errors\":[\"Invalid port mapping protocol: http\"]"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testJobCreateCommandFailsWithInvalidFilePath() throws Exception {
    when(options.getString("id")).thenReturn(JOB_ID);
    when(options.getString("image")).thenReturn("spotify/busybox:latest");
    doReturn(new File("non/existant/file")).when(options).get("file");
    runCommand();
  }

  /**
   * Test that when the environment variables we have defaults for are set, they are picked up in
   * the job metadata.
   */
  @Test
  public void testMetadataPicksUpEnvVars() throws Exception {
    envVars.put("GIT_COMMIT", "abcdef1234");

    when(options.getString("id")).thenReturn(JOB_ID);
    when(options.getString("image")).thenReturn("spotify/busybox:latest");

    when(options.getList("metadata")).thenReturn(Lists.<Object>newArrayList("foo=bar"));

    final int ret = runCommand();

    assertEquals(0, ret);
    final String output = baos.toString();

    final String expectedOutputPrefix = "Creating job: ";
    assertThat(output, startsWith(expectedOutputPrefix));

    final ObjectMapper objectMapper = new ObjectMapper();

    final String jsonFromOutput = output.split("\n")[0].substring(expectedOutputPrefix.length());
    final JsonNode jsonNode = objectMapper.readTree(jsonFromOutput);

    final ArrayList<String> fieldNames = Lists.newArrayList(jsonNode.fieldNames());
    assertThat(fieldNames, hasItem("metadata"));

    assertThat(jsonNode.get("metadata").isObject(), equalTo(true));

    final ObjectNode metadataNode = (ObjectNode) jsonNode.get("metadata");
    assertThat(metadataNode.get("foo").asText(), equalTo("bar"));
    assertThat(metadataNode.get("GIT_COMMIT").asText(), equalTo("abcdef1234"));
  }

  private CustomTypeSafeMatcher<Job> matchesName(final String name) {
    return new CustomTypeSafeMatcher<Job>("A Job with name " + name) {
      @Override
      protected boolean matchesSafely(final Job item) {
        return item.getId().getName().equals(name);
      }
    };
  }

  /**
   * Ensure that creating a job from a json file which has added and dropped capabilities is not
   * overwritten by empty arguments in the CLI switches.
   */
  @Test
  public void testAddCapabilitiesFromJsonFile() throws Exception {
    when(options.getString("id")).thenReturn(JOB_ID);
    when(options.getString("image")).thenReturn("foobar");

    when(options.get("file"))
        .thenReturn(new File("src/test/resources/job_config_extra_capabilities.json"));

    when(options.getList("add-capability")).thenReturn(Collections.emptyList());
    when(options.getList("drop-capability")).thenReturn(Collections.emptyList());

    assertEquals(0, runCommand());

    verify(client).createJob(argThat(hasCapabilities(ImmutableSet.of("cap_one", "cap_two"),
        ImmutableSet.of("cap_three", "cap_four"))
    ));
  }

  private Matcher<Job> hasCapabilities(final Set<String> added, final Set<String> dropped) {
    final String description =
        "Job with addCapabilities=" + added + " and droppedCapabilities=" + dropped;

    return new CustomTypeSafeMatcher<Job>(description) {
      @Override
      protected boolean matchesSafely(final Job actual) {
        return Objects.equals(added, actual.getAddCapabilities())
               && Objects.equals(dropped, actual.getDropCapabilities());
      }
    };
  }

  @Test
  public void testLabelsFromJsonFile() throws Exception {
    when(options.getString("id")).thenReturn(JOB_ID);
    when(options.getString("image")).thenReturn("foobar");

    when(options.get("file"))
        .thenReturn(new File("src/test/resources/job_config_extra_labels.json"));

    when(options.getList("labels")).thenReturn(Collections.emptyList());

    assertEquals(0, runCommand());

    verify(client).createJob(argThat(hasLabels(ImmutableMap.of("foo", "bar", "baz", "qux"))
    ));
  }

  private Matcher<Job> hasLabels(final Map<String, String> labels) {
    final String description = "Job with labels=" + labels;

    return new CustomTypeSafeMatcher<Job>(description) {
      @Override
      protected boolean matchesSafely(final Job actual) {
        return Objects.equals(labels, actual.getLabels());
      }
    };
  }

}

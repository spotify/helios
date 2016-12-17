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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.junit.Before;
import org.junit.Test;

public class JobInspectCommandTest {

  private static final String JOB_NAME = "foo";
  private static final String JOB_VERSION = "2-bbb";
  private static final String JOB_NAME_VERSION = JOB_NAME + ":" + JOB_VERSION;

  private static final Job JOB = Job.newBuilder()
      .setName(JOB_NAME)
      .setVersion(JOB_VERSION)
      .setCreated((long) 0)
      .setExpires(new Date(0))
      .setSecondsToWaitBeforeKill(10)
      .setAddCapabilities(ImmutableSet.of("cap1", "cap2"))
      .setDropCapabilities(ImmutableSet.of("cap3", "cap4"))
      .build();

  private final Namespace options = mock(Namespace.class);
  private final HeliosClient client = mock(HeliosClient.class);
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private final PrintStream out = new PrintStream(baos);

  private JobInspectCommand command;

  private final Map<JobId, Job> jobs = ImmutableMap.of(
      new JobId(JOB_NAME, JOB_VERSION), JOB
  );

  @Before
  public void setUp() {
    // use a real, dummy Subparser impl to avoid having to mock out every single call
    final ArgumentParser parser = ArgumentParsers.newArgumentParser("test");
    final Subparser subparser = parser.addSubparsers().addParser("inspect");
    command = new JobInspectCommand(subparser, TimeZone.getTimeZone("UTC"));

    when(client.jobs(JOB_NAME_VERSION)).thenReturn(Futures.immediateFuture(jobs));
  }

  @Test
  public void test() throws Exception {
    when(options.getString("job")).thenReturn(JOB_NAME_VERSION);
    final int ret = command.run(options, client, out, false, null);

    assertEquals(0, ret);
    final String output = baos.toString();
    assertThat(output, containsString("Created: Thu, 1 Jan 1970 00:00:00 +0000"));
    assertThat(output, containsString("Expires: Thu, 1 Jan 1970 00:00:00 +0000"));
    assertThat(output, containsString("Time to wait before kill (seconds): 10"));
    assertThat(output, containsString("Add capabilities: cap1, cap2"));
    assertThat(output, containsString("Drop capabilities: cap3, cap4"));
  }

  @Test
  public void testJson() throws Exception {
    when(options.getString("job")).thenReturn(JOB_NAME_VERSION);
    final int ret = command.run(options, client, out, true, null);

    assertEquals(0, ret);
    final String output = baos.toString();
    final Job job = Json.read(output, Job.class);
    assertEquals(JOB, job);
  }
}

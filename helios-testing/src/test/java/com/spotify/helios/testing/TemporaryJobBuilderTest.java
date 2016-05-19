/*
 * Copyright (c) 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package com.spotify.helios.testing;

import com.spotify.docker.client.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.PortMapping;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.common.io.Resources;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TemporaryJobBuilderTest {

  private static final Logger log = LoggerFactory.getLogger(TemporaryJobBuilderTest.class);

  @Rule
  public ExpectedException exception = ExpectedException.none();

  private final Deployer deployer = mock(Deployer.class);
  private final Prober prober = mock(Prober.class);
  private final Map<String, String> env = Collections.emptyMap();

  private TemporaryJobBuilder builder;
  private Job.Builder jobBuilder;

  @Before
  public void setUp() throws Exception {
    jobBuilder = Job.newBuilder()
        .setName("foo")
        .addPort("http", PortMapping.of(8080));

    builder =
        new TemporaryJobBuilder(deployer, "prefix-", prober, env, jobBuilder);

    cleanup();
  }

  /** remove the image_info.json and docker/image-name files to start/end with a clean slate */
  @After
  public void cleanup() throws Exception {
    final Set<String> pathsToDelete = ImmutableSet.of(
        "image_info.json", "target/image_info.json",
        "docker/image-name", "target/docker/image-name");

    for (final String path : pathsToDelete) {
      deleteFromClasspath(path);
      deleteFile(new File(path));
    }
  }

  private void deleteFromClasspath(final String path) throws URISyntaxException {
    final URL url;
    try {
      url = Resources.getResource(path);
    } catch (IllegalArgumentException e) {
      // file not found, cool
      return;
    }
    deleteFile(new File(url.toURI()));
  }

  private void deleteFile(final File f) {
    if (f.exists()) {
      if (f.delete()) {
        log.info("deleted {}", f.getAbsolutePath());
      } else {
        throw new IllegalStateException("Unable to delete file at " + f.getAbsolutePath());
      }
    }
  }

  @Test
  public void testBuildFromJob() {
    final ImmutableList<String> hosts = ImmutableList.of("host1");

    builder.deploy(hosts);

    final ImmutableSet<String> expectedWaitPorts = ImmutableSet.of("http");
    verify(deployer).deploy(any(Job.class), eq(hosts), eq(expectedWaitPorts),
                            eq(prober));
  }

  @Test
  public void testImageFromBuild_NoImageFiles() {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Could not find image_info.json");

    builder.imageFromBuild();
  }

  @Test
  public void testImageFromJson() throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final String fileContents = objectMapper.createObjectNode()
        .put("image", "foobar:1.2.3")
        .toString();

    writeToFile(fileContents, "target/image_info.json");

    builder.imageFromBuild();

    assertThat(jobBuilder.getImage(), is("foobar:1.2.3"));
  }

  private void writeToFile(final String fileContents, final String relativePath)
      throws IOException {
    final File f = new File(relativePath);
    Files.createParentDirs(f);
    if (!f.createNewFile()) {
      throw new IllegalStateException("Cannot create file at " + f.getAbsolutePath());
    }
    Files.write(fileContents.getBytes(Charsets.UTF_8), f);
    log.info("Wrote to file at {}", f.getAbsolutePath());
  }

  @Test
  public void testImageFromDockerfileMavenPlugin() throws Exception {
    writeToFile("foobar:from.dockerfile\n", "target/docker/image-name");

    builder.imageFromBuild();

    assertThat(jobBuilder.getImage(), is("foobar:from.dockerfile"));
  }
}

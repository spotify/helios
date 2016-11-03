/*
 * Copyright (c) 2014 Spotify AB.
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

package com.spotify.helios.agent;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.exceptions.DockerTimeoutException;
import com.spotify.docker.client.exceptions.ImageNotFoundException;
import com.spotify.docker.client.exceptions.ImagePullFailedException;
import com.spotify.helios.agent.TaskRunner.Listener;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DockerClientImagePuller implements ImagePuller {

  private static final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);

  private Listener listener;
  private DockerClient docker;

  private DockerClientImagePuller(final Listener listener, final DockerClient docker) {
    this.listener = listener;
    this.docker = docker;
  }

  static DockerClientImagePuller create(final Listener listener, final DockerClient docker) {
    return new DockerClientImagePuller(listener, docker);
  }

  public void pullImage(final String image) throws DockerException, InterruptedException {
    listener.pulling();

    DockerTimeoutException wasTimeout = null;
    final Stopwatch pullTime = Stopwatch.createStarted();

    // Attempt to pull. Failure, while less than ideal, is ok.
    try {
      docker.pull(image);
      listener.pulled();
      LOG.info("Pulled image {} in {}s", image, pullTime.elapsed(SECONDS));
    } catch (DockerTimeoutException e) {
      LOG.warn("Pulling image {} failed with timeout after {}s", image,
               pullTime.elapsed(SECONDS), e);
      listener.pullFailed();
      wasTimeout = e;
    } catch (DockerException e) {
      LOG.warn("Pulling image {} failed after {}s", image, pullTime.elapsed(SECONDS), e);
      listener.pullFailed();
    }

    try {
      // If we don't have the image by now, fail.
      docker.inspectImage(image);
    } catch (ImageNotFoundException e) {
      // If we get not found, see if we timed out above, since that's what we actually care
      // to know, as the pull should have fixed the not found-ness.
      if (wasTimeout != null) {
        throw new ImagePullFailedException("Failed pulling image " + image + " because of timeout",
                                           wasTimeout);
      }
      throw e;
    }
  }
}

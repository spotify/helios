/*-
 * -\-\-
 * Helios Testing Library
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

package com.spotify.helios.testing;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import com.spotify.helios.common.descriptors.Job;
import org.junit.Test;

public class JobsTest {

  @Test
  public void testGetJobDescription() {
    final String image = "spotify/busybox:latest";

    final Job job = Job.newBuilder()
        .setImage(image)
        .setName("testGetJobDescription")
        .setVersion("1")
        .build();
    final String shortHash = job.getId().getHash().substring(0, 7);

    // Simple test to verify the job description contains the image name and a shortened job hash.
    assertThat(Jobs.getJobDescription(job),
        both(startsWith(image)).and(containsString(shortHash)));
  }

}

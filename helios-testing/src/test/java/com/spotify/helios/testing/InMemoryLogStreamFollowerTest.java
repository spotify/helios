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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.docker.client.LogMessage;
import com.spotify.helios.common.descriptors.JobId;

import com.google.common.collect.Iterators;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class InMemoryLogStreamFollowerTest {

  private final InMemoryLogStreamFollower follower = InMemoryLogStreamFollower.create();

  private final JobId jobId = JobId.fromString("foobar:1");

  @Test
  public void testFollow() throws Exception {
    final Iterator<LogMessage> messages = Iterators.forArray(
        asMessage(LogMessage.Stream.STDOUT, "hello "),
        asMessage(LogMessage.Stream.STDERR, "error 1"),
        asMessage(LogMessage.Stream.STDOUT, "world")
    );

    follower.followLog(jobId, "1234abcd", messages);

    assertThat(new String(follower.getStdout(jobId)), is("hello world"));
    assertThat(new String(follower.getStderr(jobId)), is("error 1"));
  }

  private static LogMessage asMessage(LogMessage.Stream stream, String string) {
    return new LogMessage(stream, ByteBuffer.wrap(string.getBytes()));
  }
}

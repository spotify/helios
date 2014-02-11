/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import com.spotify.helios.cli.CliMain;
import com.spotify.helios.common.descriptors.JobId;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;

public class JobWatchExactTest extends SystemTestBase {

  @Test
  public void test() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_AGENT);
    awaitAgentStatus(TEST_AGENT, UP, LONG_WAIT_MINUTES, MINUTES);

    // Create job
    final JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", DO_NOTHING_COMMAND);

    // deploy
    deployJob(jobId, TEST_AGENT);

    final String[] commands = new String[]{"job", "watch", "--exact", "-z", masterEndpoint,
                                           "--no-log-setup", jobId.toString(), TEST_AGENT,
                                           "FAKE_TEST_AGENT"};

    final AtomicBoolean success = new AtomicBoolean(false);
    final List<String> outputLines = Lists.newArrayList();

    final long deadline = System.currentTimeMillis() + MINUTES.toMillis(LONG_WAIT_MINUTES);

    final OutputStream out = new OutputStream() {
      boolean seenKnownState;
      boolean seenUnknownAgent;
      int counter;
      final byte[] lineBuffer = new byte[8192];

      @Override
      public void write(int b) throws IOException {
        if (System.currentTimeMillis() > deadline) {
          throw new IOException("timed out trying to succeed");
        }
        lineBuffer[counter] = (byte) b;
        counter++;

        if (b != 10) {
          return;
        }

        String line = Charsets.UTF_8.decode(
            ByteBuffer.wrap(lineBuffer, 0, counter)).toString();
        outputLines.add(line);
        counter = 0;

        if (line.contains(TEST_AGENT) && !line.contains("UNKNOWN")) {
          seenKnownState = true;
        }
        if (line.contains("FAKE_TEST_AGENT") && line.contains("UNKNOWN")) {
          seenUnknownAgent = true;
        }
        if (seenKnownState && seenUnknownAgent) {
          success.set(true);
          throw new IOException("output closed");
        }
      }
    };
    final CliMain main = new CliMain(new PrintStream(out),
                                     new PrintStream(new ByteArrayOutputStream()), commands);
    main.run();
    assertTrue("Should have stopped the stream due to success: got\n"
               + Joiner.on("").join(outputLines), success.get());
  }
}

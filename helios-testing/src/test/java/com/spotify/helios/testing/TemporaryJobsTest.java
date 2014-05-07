package com.spotify.helios.testing;

import com.google.common.base.Joiner;
import com.google.protobuf.ByteString;

import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.system.SystemTestBase;
import com.spotify.hermes.Hermes;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.message.MessageBuilder;
import com.spotify.hermes.message.MessageBuilderFactory;
import com.spotify.hermes.service.Client;

import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.spotify.hermes.message.Command.REQUEST;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.experimental.results.PrintableResult.testResult;
import static org.junit.experimental.results.ResultMatchers.isSuccessful;

public class TemporaryJobsTest extends SystemTestBase {

  // These static fields exist as a way for FakeTest to access non-static fields and methods in
  // SystemTestBase. This is a bit ugly, but we can't pass the values to FakeTest, because we don't
  // instantiate it, JUnit does in the PrintableResult.testResult method. And since JUnit
  // instantiates it, it must be a static class, which means it can't access the non-static fields
  // in SystemTestBase.
  private static HeliosClient client;
  private static String testHost;

  private static final class TestProber extends DefaultProber {

    @Override
    public boolean probe(final String host, final int port) {
      // Probe for ports where docker is running instead of on the mock testHost address
      assertEquals(testHost, host);
      return super.probe(DOCKER_ADDRESS, port);
    }
  }

  public static class FakeTest {

    public static final String SERVICE = "service";
    private static final String IMAGE_NAME =
        "registry:80/spotify/wiggum:0.0.1-SNAPSHOT-c387379";

    @Rule
    public final TemporaryJobs temporaryJobs = new TemporaryJobs(client, new TestProber());

    private final TemporaryJob job1 = temporaryJobs.job()
        .image(IMAGE_NAME)
        .port(SERVICE, 4229)
        .registration("wiggum", "hm", SERVICE)
        .deploy(testHost);

    private final TemporaryJob job2 = temporaryJobs.job()
        .imageFromBuild()
        .host(testHost)
        .port("service", 4229, true)
        .registration("wiggum", "hm", SERVICE)
        .env("FOO_ADDRESS", Joiner.on(',').join(job1.addresses(SERVICE)))
        .deploy();

    @Test
    public void testDeployment() throws Exception {
      // Verify that it is possible to deploy additional jobs during test
      final TemporaryJob job3 = temporaryJobs.job()
          .image(IMAGE_NAME)
          .host(testHost)
          .deploy();

      final Map<JobId, Job> jobs = client.jobs().get(15, SECONDS);
      assertEquals("wrong number of jobs running", 3, jobs.size());
      for (Job job : jobs.values()) {
        assertEquals("wrong job running", IMAGE_NAME, job.getImage());
      }

      final MessageBuilder messageBuilder = MessageBuilderFactory
          .newBuilder("hm://wiggum/ping", REQUEST)
          .setTtlMillis(3000)
          .setMethod("GET");

      final Integer port = job1.port(testHost, SERVICE);
      assertNotNull("null external port", port);
      final Client hermesClient = Hermes.newClient(format("tcp://%s:%s/ping",
                                                          DOCKER_ADDRESS, port));
      final Message message = hermesClient.send(messageBuilder.build()).get(5, SECONDS);

      final List<ByteString> payloads = message.getPayloads();
      assertEquals("Wrong number of payloads", 1, payloads.size());
      assertEquals("Wrong payload", "PONG", payloads.get(0).toStringUtf8());
    }

  }

  @Test
  public void testRule() throws Exception {
    startDefaultMaster();
    client = defaultClient();
    testHost = getTestHost();
    startDefaultAgent(testHost);

    assertThat(testResult(FakeTest.class), isSuccessful());
    assertTrue("jobs are running that should not be",
               client.jobs().get(15, SECONDS).isEmpty());
  }

}

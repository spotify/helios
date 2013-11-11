/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios;

import com.google.common.io.Files;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.cli.CliMain;
import com.spotify.helios.common.ServiceMain;
import com.spotify.helios.master.MasterMain;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.descriptors.AgentJob;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.logging.UncaughtExceptionLogger;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.helios.common.descriptors.JobGoal.START;
import static com.spotify.helios.common.descriptors.JobStatus.State.DESTROYED;
import static com.spotify.helios.common.descriptors.JobStatus.State.RUNNING;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class SystemTest {
  private static final String BOGUS_JOB = "BOGUS_JOB";
  private static final String BOGUS_AGENT = "BOGUS_AGENT";

  private static final AtomicInteger PORT_COUNTER = new AtomicInteger(5000);

  private static final String TEST_USER = "TestUser";
  private static final String TEST_AGENT = "test-agent";

  private final int masterPort = PORT_COUNTER.incrementAndGet();
  private final String masterEndpoint = "tcp://localhost:" + masterPort;

  private final int zookeeperPort = PORT_COUNTER.incrementAndGet();
  private final String zookeeperEndpoint = "localhost:" + zookeeperPort;

  private final String dockerEndpoint = getDockerEndpoint();

  private File tempDir;
  private List<ServiceMain> mains = newArrayList();
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private ZooKeeperServer zkServer;
  private ServerCnxnFactory cnxnFactory;

  @Before
  public void setup() throws Exception {
    UncaughtExceptionLogger.setDefaultUncaughtExceptionHandler();
    tempDir = Files.createTempDir();

    startZookeeper(tempDir);
  }

  @After
  public void teardown() throws Exception {
    for (final ServiceMain main : mains) {
      main.stopAsync();
      main.awaitTerminated();
    }
    mains = null;

    stopZookeeper();

    executorService.shutdownNow();
    executorService.awaitTermination(30, SECONDS);

    deleteDirectory(tempDir);
    tempDir = null;
  }

  private void startZookeeper(final File tempDir) throws Exception {
    zkServer = new ZooKeeperServer();
    zkServer.setTxnLogFactory(new FileTxnSnapLog(tempDir, tempDir));
    cnxnFactory = ServerCnxnFactory.createFactory();
    cnxnFactory.configure(new InetSocketAddress(zookeeperPort), 0);
    cnxnFactory.startup(zkServer);
  }

  private void stopZookeeper() throws Exception {
    cnxnFactory.shutdown();
    zkServer.shutdown();
  }

  private ByteArrayOutputStream main(final String... args) throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    final CliMain main = new CliMain(new PrintStream(out), new PrintStream(err), args);
    main.run();
    return out;
  }

  private MasterMain startMaster(final String... args) throws Exception {
    final MasterMain main = new MasterMain(args);
    main.startAsync();
    main.awaitRunning();
    mains.add(main);
    return main;
  }

  private AgentMain startAgent(final String... args) throws Exception {
    final AgentMain main = new AgentMain(args);
    main.startAsync();
    main.awaitRunning();
    mains.add(main);
    return main;
  }

  private ByteArrayOutputStream main(final Collection<String> args) throws Exception {
    return main(args.toArray(new String[args.size()]));
  }

  private String control(final String command, final String sub, final Object... args)
      throws Exception {
    return control(command, sub, flatten(args));
  }

  private String control(final String command, final String sub, final String... args)
      throws Exception {
    return control(command, sub, asList(args));
  }

  private String control(final String command, final String sub, final List<String> args)
      throws Exception {
    final List<String> commands = asList(command, sub, "-z", masterEndpoint, "--no-log-setup");
    final List<String> allArgs = newArrayList(concat(commands, args));
    return main(allArgs).toString();
  }

  private void awaitAgentRegistered(final String name, final long timeout, final TimeUnit timeUnit)
      throws Exception {
    await(timeout, timeUnit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final String output = control("host", "list");
        return output.contains(name) ? true : null;
      }
    });
  }

  private void assertContains(String needle, String haystack) {
    if (!contains(needle, haystack)) {
      fail("expected to find [" + needle + "] in [" + haystack + "]");
    }
  }

  private void assertNotContains(String needle, String haystack) {
    if (contains(needle, haystack)) {
      fail("expected NOT to find [" + needle + "] in [" + haystack + "]");
    }
  }

  private boolean contains(final String needle, final String haystack) {
    return haystack.contains(needle);
  }

  private void deployJob(final String job, final String agent)
      throws Exception {
    final String deployOutput = control("job", "deploy", job, agent);
    assertContains(agent + ": done", deployOutput);

    final String listOutput = control("host", "jobs", agent);
    assertContains(job, listOutput);
  }

  private List<String> flatten(final Object... values) {
    final Iterable<Object> valuesList = asList(values);
    return flatten(valuesList);
  }

  private List<String> flatten(final Iterable<?> values) {
    final List<String> list = new ArrayList<>();
    for (Object value : values) {
      if (value instanceof Iterable) {
        list.addAll(flatten((Iterable<?>) value));
      } else if (value.getClass() == String[].class) {
        list.addAll(asList((String[]) value));
      } else if (value instanceof String) {
        list.add((String) value);
      } else {
        throw new IllegalArgumentException();
      }
    }
    return list;
  }

  private void undeployJob(final String job, final String host) throws Exception {
    final String bogusUndeployAgentWrong = control("job", "undeploy", job, BOGUS_AGENT);
    assertContains("AGENT_NOT_FOUND", bogusUndeployAgentWrong);

    final String bogusUndeployJobWrong = control("job", "undeploy", BOGUS_JOB, host);
    assertContains("JOB_NOT_FOUND", bogusUndeployJobWrong);

    final String undeployOutput = control("job", "undeploy", job, host);
    assertContains(host + ": done", undeployOutput);

    final String listOutput = control("host", "jobs", host);
    assertNotContains(job, listOutput);
  }

  @Test
  public void testService() throws Exception {
    final String agentName = "foobar";
    final String jobName = "foo";
    final String jobVersion = "17";

    startMaster("-vvvv",
              "--no-log-setup",
              "--munin-port", "0",
              "--hm", masterEndpoint,
              "--zk", zookeeperEndpoint);

    startAgent("-vvvv",
              "--no-log-setup",
              "--munin-port", "0",
              "--name", agentName,
              "--docker", dockerEndpoint,
              "--zk", zookeeperEndpoint);

    final Client control = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();

    // Create a job
    final JobDescriptor job = JobDescriptor.newBuilder()
        .setName(jobName)
        .setVersion(jobVersion)
        .setImage("busybox")
        .setCommand(asList("sh", "-c", "while :; do sleep 1; done"))
        .build();
    final CreateJobResponse created = control.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitAgent(control, agentName, 10, SECONDS);

    // Deploy the job on the agent
    final String jobId = format("%s:%s:%s", jobName, jobVersion, job.getHash());
    final AgentJob agentJob = AgentJob.of(jobId, START);
    final JobDeployResponse deployed = control.deploy(agentJob, agentName).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    final JobDeployResponse deployed2 = control.deploy(agentJob, agentName).get();
    assertEquals(JobDeployResponse.Status.JOB_ALREADY_DEPLOYED, deployed2.getStatus());

    final JobDeployResponse deployed3 = control.deploy(AgentJob.of(BOGUS_JOB, START),
        agentName).get();
    assertEquals(JobDeployResponse.Status.JOB_NOT_FOUND, deployed3.getStatus());

    final JobDeployResponse deployed4 = control.deploy(agentJob, BOGUS_AGENT).get();
    assertEquals(JobDeployResponse.Status.AGENT_NOT_FOUND, deployed4.getStatus());

    // Check that the job is in the desired state
    final AgentJob fetchedAgentJob = control.stat(agentName, jobId).get();
    assertEquals(agentJob, fetchedAgentJob);

    // Wait for the job to run
    JobStatus jobStatus;
    jobStatus = awaitJobState(control, agentName, jobId, RUNNING, 2, MINUTES);
    assertEquals(job, jobStatus.getJob());

    // Wait for a while and make sure that the container is still running
    Thread.sleep(5000);
    final AgentStatus agentStatus = control.agentStatus(agentName).get();
    jobStatus = agentStatus.getStatuses().get(jobId);
    assertEquals(RUNNING, jobStatus.getState());

    // Undeploy the container
    final JobUndeployResponse undeployed = control.undeploy(jobId, agentName).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Make sure that it is no longer in the desired state
    final AgentJob undeployedJob = control.stat(agentName, jobId).get();
    assertNull(undeployedJob);

    // Wait for the container to enter the DESTROYED state
    awaitJobState(control, agentName, jobId, DESTROYED, 10, SECONDS);
  }

  private JobStatus awaitJobState(final Client controlClient, final String slave,
                                  final String jobId,
                                  final JobStatus.State state, final int timeout,
                                  final TimeUnit timeunit) throws Exception {
    return await(timeout, timeunit, new Callable<JobStatus>() {
      @Override
      public JobStatus call() throws Exception {
        final AgentStatus agentStatus = controlClient.agentStatus(slave).get();
        final JobStatus jobStatus = agentStatus.getStatuses().get(jobId);
        return (jobStatus != null && jobStatus.getState() == state) ? jobStatus
                                                                    : null;
      }
    });
  }

  private void awaitAgent(final Client controlClient, final String slave, final int timeout,
                          final TimeUnit timeUnit) throws Exception {
    await(timeout, timeUnit, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        final AgentStatus agentStatus = controlClient.agentStatus(slave).get();
        return agentStatus != null;
      }
    });
  }

  private <T> T await(final long timeout, final TimeUnit timeUnit, final Callable<T> callable)
      throws Exception {
    final long deadline = nanoTime() + timeUnit.toNanos(timeout);
    while (nanoTime() < deadline) {
      final T value = callable.call();
      if (value != null) {
        return value;
      }
      Thread.sleep(100);
    }
    throw new TimeoutException();
  }

  @Test
  public void testServiceUsingCLI() throws Exception {
    startMaster(
        "-vvvv",
        "--no-log-setup",
        "--munin-port", "0",
        "--zk", "localhost:" + zookeeperPort,
        "--hm", masterEndpoint);

    startAgent(
        "-vvvv",
        "--no-log-setup",
        "--munin-port", "0",
        "--zk", "localhost:" + zookeeperPort,
        "--docker", dockerEndpoint,
        "--name", TEST_AGENT);

    final String jobName = "test";
    final String jobVersion = "17";
    final String jobImage = "busybox";
    final List<String> command = asList("sh", "-c", "while :; do sleep 1; done");

    // Wait for agent to come up
    awaitAgentRegistered(TEST_AGENT, 10, SECONDS);

    // Create job
    final String jobId = createJob(jobName, jobVersion, jobImage, command);

    // Deploy job
    deployJob(jobId, TEST_AGENT);

    // Undeploy job
    undeployJob(jobId, TEST_AGENT);
  }

  private String createJob(final String name, final String version, final String image,
                           final List<String> command) throws Exception {
    final String createOutput = control("job", "create", "-q", name, version, image, "--", command);
    final String jobId = StringUtils.strip(createOutput);

    final String listOutput = control("job", "list");
    assertContains(jobId, listOutput);
    return jobId;
  }

  private static String getDockerEndpoint() {
    final String endpoint = System.getenv("DOCKER_ENDPOINT");
    return endpoint == null ? "http://localhost:4160" : endpoint;
  }
}

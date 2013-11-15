/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios;

import com.kpelykh.docker.client.DockerClient;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.cli.CliMain;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.ServiceMain;
import com.spotify.helios.common.descriptors.AgentJob;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.master.MasterMain;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.helios.common.descriptors.JobGoal.START;
import static com.spotify.helios.common.descriptors.JobStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.JobStatus.State.STOPPED;
import static java.lang.String.format;
import static java.lang.System.nanoTime;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class SystemTest extends ZooKeeperTestBase {

  private static final String BOGUS_JOB = "BOGUS_JOB";
  private static final String BOGUS_AGENT = "BOGUS_AGENT";

  private static final String TEST_USER = "TestUser";
  private static final String TEST_AGENT = "test-agent";

  private final int masterPort = ZooKeeperTestBase.PORT_COUNTER.incrementAndGet();
  private final String masterEndpoint = "tcp://localhost:" + masterPort;
  private final String masterName = "test-master";

  private final String dockerEndpoint = getDockerEndpoint();

  private List<ServiceMain> mains = newArrayList();
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  @Override
  @After
  public void teardown() throws Exception {
    for (final ServiceMain main : mains) {
      main.stopAsync();
      main.awaitTerminated();
    }
    mains = null;

    executorService.shutdownNow();
    executorService.awaitTermination(30, SECONDS);

    super.teardown();
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
    assertThat(haystack, containsString(needle));
  }

  private void assertNotContains(String needle, String haystack) {
    assertThat(haystack, not(containsString(needle)));
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

    startDefaultMaster();

    final Client control = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();

    AgentStatus v = control.agentStatus(agentName).get();
    assertNull(v); // for NOT_FOUND

    startDefaultAgent(agentName);

    List<String> command = asList("sh", "-c", "while :; do sleep 1; done");
    // Create a job
    final JobDescriptor job = JobDescriptor.newBuilder()
        .setName(jobName)
        .setVersion(jobVersion)
        .setImage("busybox")
        .setCommand(command)
        .build();
    final CreateJobResponse created = control.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final CreateJobResponse duplicateJob = control.createJob(job).get();
    assertEquals(CreateJobResponse.Status.JOB_ALREADY_EXISTS, duplicateJob.getStatus());

    final CreateJobResponse createIdMismatch = control.createJob(
        new JobDescriptor("foo", jobName, jobVersion, "busyBox", command) {
          @Override
          public String getId() { return "BOOO"; }
        }).get();
    assertEquals(CreateJobResponse.Status.ID_MISMATCH, createIdMismatch.getStatus());

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

    assertEquals(JobDeleteResponse.Status.STILL_IN_USE, control.deleteJob(jobId).get().getStatus());

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

    // Wait for the container to enter the STOPPED state
    awaitJobState(control, agentName, jobId, STOPPED, 10, SECONDS);

    assertEquals(JobDeleteResponse.Status.OK, control.deleteJob(jobId).get().getStatus());
  }

  private void startDefaultMaster() throws Exception {
    startMaster("-vvvv",
                "--no-log-setup",
                "--munin-port", "0",
                "--hm", masterEndpoint,
                "--name", masterName,
                "--zk", zookeeperEndpoint);
  }

  private AgentMain startDefaultAgent(final String agentName) throws Exception {
    return startAgent("-vvvv",
                      "--no-log-setup",
                      "--munin-port", "0",
                      "--name", agentName,
                      "--docker", dockerEndpoint,
                      "--zk", zookeeperEndpoint);
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
    await(timeout, timeUnit, new Callable<AgentStatus>() {
      @Override
      public AgentStatus call() throws Exception {
        return controlClient.agentStatus(slave).get();
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
    startDefaultMaster();

    String output = control("master", "list");
    assertContains(masterName, output);

    assertContains("NOT_FOUND", deleteAgent(TEST_AGENT));

    startDefaultAgent(TEST_AGENT);

    final String jobName = "test";
    final String jobVersion = "17";
    final String jobImage = "busybox";
    final List<String> command = asList("sh", "-c", "while :; do sleep 1; done");

    // Wait for agent to come up
    awaitAgentRegistered(TEST_AGENT, 10, SECONDS);

    // Create job
    final String jobId = createJob(jobName, jobVersion, jobImage, command);

    final String duplicateJob = control(
        "job", "create", jobName, jobVersion, jobImage, "--", command);
    assertContains("JOB_ALREADY_EXISTS", duplicateJob);

    final String prestop = stopJob(jobId, TEST_AGENT);
    assertContains("JOB_NOT_DEPLOYED", prestop);

    // Deploy job
    deployJob(jobId, TEST_AGENT);

    // Stop job
    final String stop1 = stopJob(jobId, BOGUS_AGENT);
    assertContains("AGENT_NOT_FOUND", stop1);
    final String stop2 = stopJob(BOGUS_JOB, TEST_AGENT);
    assertContains("JOB_NOT_FOUND", stop2);
    final String stop3 = stopJob(jobId, TEST_AGENT);
    assertContains(TEST_AGENT + ": done", stop3);

    // Undeploy job
    undeployJob(jobId, TEST_AGENT);

    assertContains(TEST_AGENT + ": done", deleteAgent(TEST_AGENT));
  }

  /**
   * Verifies that:
   *
   * 1. The container is kept running when the agent is restarted.
   * 2. A container that died while the agent was down is restarted when the agent comes up.
   * 3. The container for a job that was undeployed while the agent was down is killed when the
   * agent comes up again.
   */
  @Test
  public void testAgentRestart() throws Exception {
    final String agentName = "foobar";
    final String jobName = "foo";
    final String jobVersion = "17";

    startDefaultMaster();

    final Client client = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();

    final AgentMain agent1 = startDefaultAgent(agentName);

    // A simple netcat echo server
    final List<String> command =
        asList("bash", "-c",
               "DEBIAN_FRONTEND=noninteractive apt-get install -q -y nmap && ncat -l -p 4711 -c \"while true; do read i && echo $i; done\"");

    // TODO (dano): connect to the server during the test and verify that the connection is never broken

    // Create a job
    final JobDescriptor job = JobDescriptor.newBuilder()
        .setName(jobName)
        .setVersion(jobVersion)
        .setImage("ubuntu:12.04")
        .setCommand(command)
        .build();
    final String jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitAgent(client, agentName, 10, SECONDS);

    // Deploy the job on the agent
    final AgentJob agentJob = AgentJob.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(agentJob, agentName).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    final JobStatus firstJobStatus = awaitJobState(client, agentName, jobId, RUNNING, 2, MINUTES);
    assertEquals(job, firstJobStatus.getJob());

    // Stop the agent
    agent1.stopAsync().awaitTerminated();

    // TODO: Wait for the agent to become DOWN
    Thread.sleep(1000);

    // Start the agent again
    final AgentMain agent2 = startDefaultAgent(agentName);

    // Wait for a while and make sure that the same container is still running
    Thread.sleep(5000);
    final AgentStatus agentStatus = client.agentStatus(agentName).get();
    final JobStatus jobStatus = agentStatus.getStatuses().get(jobId);
    assertEquals(RUNNING, jobStatus.getState());
    assertEquals(firstJobStatus.getId(), jobStatus.getId());

    // Stop the agent
    agent2.stopAsync().awaitTerminated();

    // Kill the container
    final DockerClient dockerClient = new DockerClient(dockerEndpoint);
    dockerClient.stopContainer(firstJobStatus.getId());

    // Start the agent again
    final AgentMain agent3 = startDefaultAgent(agentName);

    // Wait for the job to be restarted in a new container
    await(1, MINUTES, new Callable<JobStatus>() {
      @Override
      public JobStatus call() throws Exception {
        final AgentStatus agentStatus = client.agentStatus(agentName).get();
        final JobStatus jobStatus = agentStatus.getStatuses().get(jobId);
        return (jobStatus != null && jobStatus.getId() != null && !jobStatus.getId().equals(firstJobStatus.getId())) ? jobStatus
                                                                                  : null;
      }
    });

    // Stop the agent
    agent3.stopAsync().awaitTerminated();

    // TODO: Wait for the agent to become DOWN
    Thread.sleep(1000);

    // Undeploy the container
    final JobUndeployResponse undeployed = client.undeploy(jobId, agentName).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Start the agent again
    startDefaultAgent(agentName);

    // Wait for the job to enter the STOPPED state
    awaitJobState(client, agentName, jobId, STOPPED, 10, SECONDS);
  }

  private String stopJob(String jobId, String agent) throws Exception {
    return control("job", "stop", jobId, agent);
  }

  private String deleteAgent(String testAgent) throws Exception {
    return control("host", "delete", testAgent, "yes");
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

/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kpelykh.docker.client.DockerClient;
import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.utils.LogReader;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.cli.CliMain;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.ServiceMain;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.TaskStatusEvent;
import com.spotify.helios.common.protocol.TaskStatusEvents;
import com.spotify.helios.master.MasterMain;
import com.spotify.hermes.Hermes;
import com.spotify.nameless.Service;
import com.spotify.nameless.api.EndpointFilter;
import com.spotify.nameless.api.NamelessClient;
import com.spotify.nameless.proto.Messages;
import com.sun.jersey.api.client.ClientResponse;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.helios.common.descriptors.AgentStatus.Status.DOWN;
import static com.spotify.helios.common.descriptors.AgentStatus.Status.UP;
import static com.spotify.helios.common.descriptors.Goal.START;
import static com.spotify.helios.common.descriptors.TaskStatus.State.EXITED;
import static com.spotify.helios.common.descriptors.TaskStatus.State.RUNNING;
import static com.spotify.helios.common.descriptors.TaskStatus.State.STOPPED;
import static java.lang.System.nanoTime;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class SystemTest extends ZooKeeperTestBase {

  private static final JobId BOGUS_JOB = new JobId("bogus", "job", "badfood");
  private static final String BOGUS_AGENT = "BOGUS_AGENT";

  private static final String TEST_USER = "TestUser";
  private static final String TEST_AGENT = "test-agent";

  private final int masterPort = ZooKeeperTestBase.PORT_COUNTER.incrementAndGet();
  private final String masterEndpoint = "tcp://localhost:" + masterPort;
  private final String masterName = "test-master";

  private final String dockerEndpoint = getDockerEndpoint();

  private List<ServiceMain> mains = newArrayList();
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private Service nameless;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    nameless = new Service();
    nameless.start();
  }

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
        final String output = control("host", "list", "-q");
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

  private void deployJob(final JobId jobId, final String agent)
      throws Exception {
    final String deployOutput = control("job", "deploy", jobId.toString(), agent);
    assertContains(agent + ": done", deployOutput);

    final String listOutput = control("host", "jobs", "-q", agent);
    assertContains(jobId.toString(), listOutput);
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

  private void undeployJob(final JobId jobId, final String host) throws Exception {
    final String bogusUndeployAgentWrong =
        control("job", "undeploy", jobId.toString(), BOGUS_AGENT);
    assertContains("AGENT_NOT_FOUND", bogusUndeployAgentWrong);

    final String bogusUndeployJobWrong = control("job", "undeploy", BOGUS_JOB.toString(), host);
    assertContains("Unknown job", bogusUndeployJobWrong);

    final String undeployOutput = control("job", "undeploy", jobId.toString(), host);
    assertContains(host + ": done", undeployOutput);

    final String listOutput = control("host", "jobs", "-q", host);
    assertNotContains(jobId.toString(), listOutput);
  }

  @Test
  public void testNamelessRegistration() throws Exception {
    startMaster("-vvvv",
                "--no-log-setup",
                "--munin-port", "0",
                "--site", "localhost",
                "--http", "0.0.0.0:5555",
                "--hm", masterEndpoint,
                "--zk", zookeeperEndpoint);

    // sleep for half a second to give master time to register with nameless
    Thread.sleep(500);
    NamelessClient client = new NamelessClient(Hermes.newClient("tcp://localhost:4999"));
    List<Messages.RegistryEntry> entries = client.queryEndpoints(EndpointFilter.everything()).get();

    assertEquals("wrong number of nameless entries", 2, entries.size());

    boolean httpFound = false;
    boolean hermesFound = false;

    for (Messages.RegistryEntry entry : entries) {
      final Messages.Endpoint endpoint = entry.getEndpoint();
      assertEquals("wrong service", "helios", endpoint.getService());
      final String protocol = endpoint.getProtocol();

      switch (protocol) {
        case "hm":
          hermesFound = true;
          assertEquals("wrong port", endpoint.getPort(), masterPort);
          break;
        case "http":
          httpFound = true;
          assertEquals("wrong port", endpoint.getPort(), 5555);
          break;
        default:
          fail("unknown protocol " + protocol);
      }
    }

    assertTrue("missing hermes nameless entry", hermesFound);
    assertTrue("missing http nameless entry", httpFound);
  }

  @Test
  public void testJobHistory() throws Exception {
    startDefaultMaster();

    final Client control = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();

    startDefaultAgent(TEST_AGENT);
    JobId jobId = createJob("JOB_NAME", "JOB_VERSION", "busybox", ImmutableList.of("/bin/true"));
    deployJob(jobId, TEST_AGENT);
    awaitJobState(control, TEST_AGENT, jobId, EXITED, 10, SECONDS);
    undeployJob(jobId, TEST_AGENT);
    TaskStatusEvents events = control.jobHistory(jobId).get();
    List<TaskStatusEvent> eventsList = events.getEvents();
    assertFalse(eventsList.isEmpty());

    final TaskStatusEvent event1 = eventsList.get(0);
    assertEquals(State.CREATING, event1.getStatus().getState());
    assertNull(event1.getStatus().getContainerId());

    final TaskStatusEvent event2 = eventsList.get(1);
    assertEquals(State.STARTING, event2.getStatus().getState());
    assertNotNull(event2.getStatus().getContainerId());

    final TaskStatusEvent event3 = eventsList.get(2);
    assertEquals(State.RUNNING, event3.getStatus().getState());

    final TaskStatusEvent event4 = eventsList.get(3);
    assertEquals(State.EXITED, event4.getStatus().getState());
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

    final AgentMain agent = startDefaultAgent(agentName);

    List<String> command = asList("sh", "-c", "while :; do sleep 1; done");
    // Create a job
    final Job job = Job.newBuilder()
        .setName(jobName)
        .setVersion(jobVersion)
        .setImage("busybox")
        .setCommand(command)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = control.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    final CreateJobResponse duplicateJob = control.createJob(job).get();
    assertEquals(CreateJobResponse.Status.JOB_ALREADY_EXISTS, duplicateJob.getStatus());

    // Try querying for the job
    final Map<JobId, Job> noMatchJobs = control.jobs(jobName + "not_matching").get();
    assertTrue(noMatchJobs.isEmpty());

    final Map<JobId, Job> matchJobs1 = control.jobs(jobName).get();
    assertEquals(ImmutableMap.of(jobId, job), matchJobs1);

    final Map<JobId, Job> matchJobs2 = control.jobs(jobName + ":" + jobVersion).get();
    assertEquals(ImmutableMap.of(jobId, job), matchJobs2);

    final Map<JobId, Job> matchJobs3 = control.jobs(job.getId().toString()).get();
    assertEquals(ImmutableMap.of(jobId, job), matchJobs3);

    // TODO: reimplement this test
//    final CreateJobResponse createIdMismatch = control.createJob(
//        new Job("foo", jobName, jobVersion, "busyBox", command) {
//          @Override
//          public JobId getId() { return JobId.fromString("bad:job:deadbeef"); }
//        }).get();
//    assertEquals(CreateJobResponse.Status.ID_MISMATCH, createIdMismatch.getStatus());

    // Wait for agent to come up
    awaitAgentRegistered(control, agentName, 10, SECONDS);
    awaitAgentStatus(control, agentName, UP, 10, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = control.deploy(deployment, agentName).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    final JobDeployResponse deployed2 = control.deploy(deployment, agentName).get();
    assertEquals(JobDeployResponse.Status.JOB_ALREADY_DEPLOYED, deployed2.getStatus());

    final JobDeployResponse deployed3 = control.deploy(Deployment.of(BOGUS_JOB, START),
                                                       agentName).get();
    assertEquals(JobDeployResponse.Status.JOB_NOT_FOUND, deployed3.getStatus());

    final JobDeployResponse deployed4 = control.deploy(deployment, BOGUS_AGENT).get();
    assertEquals(JobDeployResponse.Status.AGENT_NOT_FOUND, deployed4.getStatus());

    // Check that the job is in the desired state
    final Deployment fetchedDeployment = control.stat(agentName, jobId).get();
    assertEquals(deployment, fetchedDeployment);

    // Wait for the job to run
    TaskStatus taskStatus;
    taskStatus = awaitJobState(control, agentName, jobId, RUNNING, 2, MINUTES);
    assertEquals(job, taskStatus.getJob());

    assertEquals(JobDeleteResponse.Status.STILL_IN_USE, control.deleteJob(jobId).get().getStatus());

    // Wait for a while and make sure that the container is still running
    Thread.sleep(5000);
    final AgentStatus agentStatus = control.agentStatus(agentName).get();
    taskStatus = agentStatus.getStatuses().get(jobId);
    assertEquals(RUNNING, taskStatus.getState());

    // Undeploy the container
    final JobUndeployResponse undeployed = control.undeploy(jobId, agentName).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Make sure that it is no longer in the desired state
    final Deployment undeployedJob = control.stat(agentName, jobId).get();
    assertNull(undeployedJob);

    // Wait for the container to enter the STOPPED state
    awaitJobState(control, agentName, jobId, STOPPED, 10, SECONDS);

    // Verify that the job can be deleted
    assertEquals(JobDeleteResponse.Status.OK, control.deleteJob(jobId).get().getStatus());

    // Stop agent and verify that the agent status changes to DOWN
    agent.stopAsync().awaitTerminated();
    awaitAgentStatus(control, agentName, DOWN, 10, SECONDS);
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
                      "--zk", zookeeperEndpoint,
                      "--zk-session-timeout", "100");
  }

  private TaskStatus awaitJobState(final Client controlClient, final String slave,
                                   final JobId jobId,
                                   final TaskStatus.State state, final int timeout,
                                   final TimeUnit timeunit) throws Exception {
    return await(timeout, timeunit, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final AgentStatus agentStatus = controlClient.agentStatus(slave).get();
        final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
        return (taskStatus != null && taskStatus.getState() == state) ? taskStatus
                                                                      : null;
      }
    });
  }

  private void awaitAgentRegistered(final Client controlClient, final String slave,
                                    final int timeout,
                                    final TimeUnit timeUnit) throws Exception {
    await(timeout, timeUnit, new Callable<AgentStatus>() {
      @Override
      public AgentStatus call() throws Exception {
        return controlClient.agentStatus(slave).get();
      }
    });
  }

  private AgentStatus awaitAgentStatus(final Client controlClient, final String slave,
                                       final AgentStatus.Status status,
                                       final int timeout,
                                       final TimeUnit timeUnit) throws Exception {
    return await(timeout, timeUnit, new Callable<AgentStatus>() {
      @Override
      public AgentStatus call() throws Exception {
        final AgentStatus agentStatus = controlClient.agentStatus(slave).get();
        if (agentStatus == null) {
          return null;
        }
        return (agentStatus.getStatus() == status) ? agentStatus : null;
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
      Thread.sleep(500);
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
    final JobId jobId = createJob(jobName, jobVersion, jobImage, command);

    // Query for job
    assertContains(jobId.toString(), control("job", "list", jobName, "-q"));
    assertContains(jobId.toString(), control("job", "list", jobName + ":" + jobVersion, "-q"));
    assertTrue(control("job", "list", "foozbarz", "-q").trim().isEmpty());

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
    assertContains("Unknown job", stop2);
    final String stop3 = stopJob(jobId, TEST_AGENT);
    assertContains(TEST_AGENT + ": done", stop3);

    // Undeploy job
    undeployJob(jobId, TEST_AGENT);

    assertContains(TEST_AGENT + ": done", deleteAgent(TEST_AGENT));
  }

  private TaskStatus awaitTaskState(final JobId jobId, final String agent,
                                    final TaskStatus.State state) throws Exception {
    long timeout = 10;
    TimeUnit timeUnit = TimeUnit.SECONDS;
    return await(timeout, timeUnit, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final String output = control("job", "status", "--json", jobId.toString());
        final Map<JobId, JobStatus> statusMap;
        try {
          statusMap = Json.read(output, new TypeReference<Map<JobId, JobStatus>>() {});
        } catch (IOException e) {
          return null;
        }
        final JobStatus status = statusMap.get(jobId);
        if (status == null) {
          return null;
        }
        final TaskStatus taskStatus = status.getTaskStatuses().get(agent);
        if (taskStatus == null) {
          return null;
        }
        if (taskStatus.getState() != state) {
          return null;
        }
        return taskStatus;
      }
    });
  }

  @Test
  public void testEnvVariables() throws Exception {
    startDefaultMaster();
    AgentMain agent = startAgent("-vvvv",
                                 "--no-log-setup",
                                 "--munin-port", "0",
                                 "--name", TEST_AGENT,
                                 "--docker", dockerEndpoint,
                                 "--zk", zookeeperEndpoint,
                                 "--zk-session-timeout", "100",
                                 "--env",
                                 "SPOTIFY_POD=PODNAME",
                                 "SPOTIFY_ROLE=ROLENAME",
                                 "BAR=badfood");

    final DockerClient dockerClient = new DockerClient(dockerEndpoint);

    final List<String> command = asList("sh", "-c",
                                        "echo pod: $SPOTIFY_POD role: $SPOTIFY_ROLE foo: $FOO bar: $BAR");

    // Create job
    final JobId jobId = createJob("NAME", "VERSION", "busybox", command,
                                  ImmutableMap.of("FOO", "4711",
                                                  "BAR", "deadbeef"));

    // deploy
    deployJob(jobId, TEST_AGENT);

    final TaskStatus taskStatus = awaitTaskState(jobId, TEST_AGENT, EXITED);

    final ClientResponse response = dockerClient.logContainer(taskStatus.getContainerId());
    final String logMessage = readLogFully(response);

    assertContains("pod: PODNAME", logMessage);
    assertContains("role: ROLENAME", logMessage);
    assertContains("foo: 4711", logMessage);

    // Verify that the the BAR environment variable in the job overrode the agent config
    assertContains("bar: deadbeef", logMessage);

    // Stop the agent
    agent.stopAsync().awaitTerminated();

    final Client client = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();

    awaitAgentStatus(client, TEST_AGENT, DOWN, 10, SECONDS);

  }

  private String readLogFully(final ClientResponse logs) throws IOException {
    final LogReader logReader = new LogReader(logs.getEntityInputStream());
    StringBuilder stringBuilder = new StringBuilder();
    LogReader.Frame frame;
    while ((frame = logReader.readFrame()) != null) {
      stringBuilder.append(UTF_8.decode(frame.getBytes()));
    }
    logReader.close();
    return stringBuilder.toString();
  }

  /**
   * Verifies that:
   *
   * 1. The container is kept running when the agent is restarted.
   *
   * 2. A container that died while the agent was down is restarted when the agent comes up.
   *
   * 3. A container that was destroyed while the agent was down is restarted when the agent comes
   * up.
   *
   * 4. The container for a job that was undeployed while the agent was down is killed when the
   * agent comes up again.
   */
  @Test
  public void testAgentRestart() throws Exception {
    final String agentName = "foobar";
    final String jobName = "foo";
    final String jobVersion = "17";

    startDefaultMaster();

    final DockerClient dockerClient = new DockerClient(dockerEndpoint);

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
    final Job job = Job.newBuilder()
        .setName(jobName)
        .setVersion(jobVersion)
        .setImage("ubuntu:12.04")
        .setCommand(command)
        .build();
    final JobId jobId = job.getId();
    final CreateJobResponse created = client.createJob(job).get();
    assertEquals(CreateJobResponse.Status.OK, created.getStatus());

    // Wait for agent to come up
    awaitAgentRegistered(client, agentName, 10, SECONDS);
    awaitAgentStatus(client, agentName, UP, 10, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, agentName).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    final TaskStatus firstTaskStatus = awaitJobState(client, agentName, jobId, RUNNING, 2, MINUTES);
    assertEquals(job, firstTaskStatus.getJob());

    // Stop the agent
    agent1.stopAsync().awaitTerminated();
    awaitAgentStatus(client, agentName, DOWN, 10, SECONDS);

    // Start the agent again
    final AgentMain agent2 = startDefaultAgent(agentName);
    awaitAgentStatus(client, agentName, UP, 10, SECONDS);

    // Wait for a while and make sure that the same container is still running
    Thread.sleep(5000);
    final AgentStatus agentStatus = client.agentStatus(agentName).get();
    final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
    assertEquals(RUNNING, taskStatus.getState());
    assertEquals(firstTaskStatus.getContainerId(), taskStatus.getContainerId());

    // Stop the agent
    agent2.stopAsync().awaitTerminated();
    awaitAgentStatus(client, agentName, DOWN, 10, SECONDS);

    // Kill the container
    dockerClient.stopContainer(firstTaskStatus.getContainerId());

    // Start the agent again
    final AgentMain agent3 = startDefaultAgent(agentName);
    awaitAgentStatus(client, agentName, UP, 10, SECONDS);

    // Wait for the job to be restarted in a new container
    final TaskStatus secondTaskStatus = await(1, MINUTES, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final AgentStatus agentStatus = client.agentStatus(agentName).get();
        final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
        return (taskStatus != null && taskStatus.getContainerId() != null &&
                taskStatus.getState() == RUNNING &&
                !taskStatus.getContainerId().equals(firstTaskStatus.getContainerId())) ? taskStatus
                                                                                       : null;
      }
    });

    // Stop the agent
    agent3.stopAsync().awaitTerminated();
    awaitAgentStatus(client, agentName, DOWN, 10, SECONDS);

    // Kill and destroy the container
    dockerClient.stopContainer(secondTaskStatus.getContainerId());
    dockerClient.removeContainer(secondTaskStatus.getContainerId());
    try {
      // This should fail with an exception if the container still exists
      dockerClient.inspectContainer(secondTaskStatus.getContainerId());
      fail();
    } catch (DockerException ignore) {
    }

    // Start the agent again
    final AgentMain agent4 = startDefaultAgent(agentName);
    awaitAgentStatus(client, agentName, UP, 10, SECONDS);

    // Wait for the job to be restarted in a new container
    await(1, MINUTES, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final AgentStatus agentStatus = client.agentStatus(agentName).get();
        final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
        return (taskStatus != null && taskStatus.getContainerId() != null &&
                taskStatus.getState() == RUNNING &&
                !taskStatus.getContainerId().equals(secondTaskStatus.getContainerId())) ? taskStatus
                                                                                        : null;
      }
    });

    // Stop the agent
    agent4.stopAsync().awaitTerminated();
    awaitAgentStatus(client, agentName, DOWN, 10, SECONDS);

    // Undeploy the container
    final JobUndeployResponse undeployed = client.undeploy(jobId, agentName).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Start the agent again
    startDefaultAgent(agentName);
    awaitAgentStatus(client, agentName, UP, 10, SECONDS);

    // Wait for the job to enter the STOPPED state
    awaitJobState(client, agentName, jobId, STOPPED, 10, SECONDS);
  }

  private String stopJob(final JobId jobId, final String agent) throws Exception {
    return control("job", "stop", jobId.toString(), agent);
  }

  private String deleteAgent(final String testAgent) throws Exception {
    return control("host", "delete", testAgent, "yes");
  }

  private JobId createJob(final String name, final String version, final String image,
                          final List<String> command) throws Exception {
    return createJob(name, version, image, command, new HashMap<String, String>());
  }

  private JobId createJob(final String name, final String version, final String image,
                          final List<String> command, final Map<String, String> env)
      throws Exception {
    final List<String> args = Lists.newArrayList("-q", name, version, image);

    if (!env.isEmpty()) {
      args.add("--env");
      for (final Map.Entry<String, String> entry : env.entrySet()) {
        args.add(entry.getKey() + "=" + entry.getValue());
      }
    }

    args.add("--");
    args.addAll(command);

    final String createOutput = control("job", "create", args);
    final String jobId = StringUtils.strip(createOutput);

    final String listOutput = control("job", "list", "-q");
    assertContains(jobId, listOutput);
    return JobId.fromString(jobId);
  }

  private static String getDockerEndpoint() {
    final String endpoint = System.getenv("DOCKER_ENDPOINT");
    return endpoint == null ? "http://localhost:4160" : endpoint;
  }
}

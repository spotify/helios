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
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.TaskStatus.State;
import com.spotify.helios.common.descriptors.ThrottleState;
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
import org.json.JSONObject;
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
import static com.spotify.helios.common.descriptors.Goal.STOP;
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

  private static final int WAIT_TIMEOUT_SECONDS = 40;
  private static final int LONG_WAIT_MINUTES = 2;
  private static final String NAMELESS_SERVICE = "service";
  private static final int INTERNAL_PORT = 4444;
  private static final int EXTERNAL_PORT = 5555;
  private static final ImmutableMap<String, String> EMPTY_ENV = ImmutableMap.<String, String>of();
  private static final JobId BOGUS_JOB = new JobId("bogus", "job", "badfood");
  private static final String BOGUS_AGENT = "BOGUS_AGENT";

  private static final String TEST_USER = "TestUser";
  private static final String TEST_AGENT = "test-agent";
  private static final List<String> DO_NOTHING_COMMAND = asList("sh", "-c",
      "while :; do sleep 1; done");

  public static final TypeReference<Map<JobId, JobStatus>> STATUSES_TYPE =
      new TypeReference<Map<JobId, JobStatus>>() {};

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
  public void testMasterNamelessRegistration() throws Exception {
    startMaster("-vvvv",
                "--no-log-setup",
                "--munin-port", "0",
                "--site", "localhost",
                "--http", "0.0.0.0:" + EXTERNAL_PORT,
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
          assertEquals("wrong port", endpoint.getPort(), EXTERNAL_PORT);
          break;
        default:
          fail("unknown protocol " + protocol);
      }
    }

    assertTrue("missing hermes nameless entry", hermesFound);
    assertTrue("missing http nameless entry", httpFound);
  }

  @Test
  public void testContainerNamelessRegistration() throws Exception {
    startDefaultMaster();

    final Client control = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();

    startAgent("-vvvv",
               "--no-log-setup",
               "--munin-port", "0",
               "--name", TEST_AGENT,
               "--docker", dockerEndpoint,
               "--zk", zookeeperEndpoint,
               "--site", "localhost",
               "--zk-session-timeout", "100");

    ImmutableMap<String, PortMapping> portMapping = ImmutableMap.<String, PortMapping>of(
        "PROTOCOL", PortMapping.of(INTERNAL_PORT, EXTERNAL_PORT));
    final JobId jobId = createJob("JOB_NAME", "JOB_VERSION", "busybox", DO_NOTHING_COMMAND,
        EMPTY_ENV, portMapping,
        NAMELESS_SERVICE);

    deployJob(jobId, TEST_AGENT);
    awaitJobState(control, TEST_AGENT, jobId, RUNNING, WAIT_TIMEOUT_SECONDS, SECONDS);
    // Give it some time for the registration to register.
    Thread.sleep(1000);
    try {
      final NamelessClient client = new NamelessClient(Hermes.newClient("tcp://localhost:4999"));
      final List<Messages.RegistryEntry> entries = client.queryEndpoints(
          EndpointFilter.everything()).get();

      boolean portFound = false;

      for (Messages.RegistryEntry entry : entries) {
        final Messages.Endpoint endpoint = entry.getEndpoint();
        assertEquals("wrong service", NAMELESS_SERVICE, endpoint.getService());
        final String protocol = endpoint.getProtocol();

        switch (protocol) {
          case "PROTOCOL":
            portFound = true;
            assertEquals("wrong port", endpoint.getPort(), EXTERNAL_PORT);
            break;
          default:
            fail("unknown protocol " + protocol);
        }
      }
      assertTrue("Did not find nameless registered port", portFound);
    } finally {  // if this isn't done, your docker will be unhappy across subsequent test runs
      undeployJob(jobId, TEST_AGENT);
    }
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
    awaitJobState(control, TEST_AGENT, jobId, EXITED, WAIT_TIMEOUT_SECONDS, SECONDS);
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
  public void testFlapping() throws Exception {
    startDefaultMaster();
    startDefaultAgent(TEST_AGENT);
    JobId jobId = createJob("JOB_NAME", "JOB_VERSION", "busybox", ImmutableList.of("/bin/true"));
    deployJob(jobId, TEST_AGENT);
    final Client control = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();
   awaitJobThrottle(control, TEST_AGENT, jobId, ThrottleState.FLAPPING, WAIT_TIMEOUT_SECONDS, SECONDS);
  }


  @Test
  public void testPortCollision() throws Exception {
    final String agentName = "foobar";
    final int externalPort = 4711;

    startDefaultMaster();
    startDefaultAgent(agentName);

    final Client control = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();

    final Job job1 = Job.newBuilder()
        .setName("foo")
        .setVersion("1")
        .setImage("busybox")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(ImmutableMap.of("foo", PortMapping.of(10001, externalPort)))
        .build();

    final Job job2 = Job.newBuilder()
        .setName("bar")
        .setVersion("1")
        .setImage("busybox")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(ImmutableMap.of("foo", PortMapping.of(10002, externalPort)))
        .build();

    final CreateJobResponse created1 = control.createJob(job1).get();
    assertEquals(CreateJobResponse.Status.OK, created1.getStatus());

    final CreateJobResponse created2 = control.createJob(job2).get();
    assertEquals(CreateJobResponse.Status.OK, created2.getStatus());

    final Deployment deployment1 = Deployment.of(job1.getId(), STOP);
    final JobDeployResponse deployed1 = control.deploy(deployment1, agentName).get();
    assertEquals(JobDeployResponse.Status.OK, deployed1.getStatus());

    final Deployment deployment2 = Deployment.of(job2.getId(), STOP);
    final JobDeployResponse deployed2 = control.deploy(deployment2, agentName).get();
    assertEquals(JobDeployResponse.Status.PORT_CONFLICT, deployed2.getStatus());
  }

  @Test
  public void testService() throws Exception {
    final String agentName = "foobar";
    final String jobName = "foo";
    final String jobVersion = "17";
    final Map<String, PortMapping> ports = ImmutableMap.of("foos", PortMapping.of(17, 4711));

    startDefaultMaster();

    final Client control = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();

    AgentStatus v = control.agentStatus(agentName).get();
    assertNull(v); // for NOT_FOUND

    final AgentMain agent = startDefaultAgent(agentName);

    // Create a job
    final Job job = Job.newBuilder()
        .setName(jobName)
        .setVersion(jobVersion)
        .setImage("busybox")
        .setCommand(DO_NOTHING_COMMAND)
        .setPorts(ports)
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
    awaitAgentRegistered(control, agentName, WAIT_TIMEOUT_SECONDS, SECONDS);
    awaitAgentStatus(control, agentName, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

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
    taskStatus = awaitJobState(control, agentName, jobId, RUNNING, LONG_WAIT_MINUTES, MINUTES);
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
    awaitJobState(control, agentName, jobId, STOPPED, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Verify that the job can be deleted
    assertEquals(JobDeleteResponse.Status.OK, control.deleteJob(jobId).get().getStatus());

    // Stop agent and verify that the agent status changes to DOWN
    agent.stopAsync().awaitTerminated();
    awaitAgentStatus(control, agentName, DOWN, WAIT_TIMEOUT_SECONDS, SECONDS);
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

  private TaskStatus awaitJobThrottle(final Client controlClient, final String slave,
                                      final JobId jobId,
                                      final ThrottleState throttled, final int timeout,
                                      final TimeUnit timeunit) throws Exception {
      return await(timeout, timeunit, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final AgentStatus agentStatus = controlClient.agentStatus(slave).get();
        final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
        return (taskStatus != null && taskStatus.getThrottled() == throttled) ? taskStatus : null;
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

    final String name = "test";
    final String version = "17";
    final String image = "busybox";
    final Map<String, PortMapping> ports = ImmutableMap.of("foo", PortMapping.of(4711),
                                                           "bar", PortMapping.of(5000, 6000));
    final Map<String, String> env = ImmutableMap.of("BAD", "f00d");
    // Wait for agent to come up
    awaitAgentRegistered(TEST_AGENT, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Create job
    final JobId jobId = createJob(name, version, image, DO_NOTHING_COMMAND, env, ports);

    // Query for job
    assertContains(jobId.toString(), control("job", "list", name, "-q"));
    assertContains(jobId.toString(), control("job", "list", name + ":" + version, "-q"));
    assertTrue(control("job", "list", "foozbarz", "-q").trim().isEmpty());

    // Verify that port mapping and environment variables are correct
    final String statusString = control("job", "status", jobId.toString(), "--json");
    final Map<JobId, JobStatus> statuses = Json.read(statusString, STATUSES_TYPE);
    final Job job = statuses.get(jobId).getJob();
    assertEquals(4711, job.getPorts().get("foo").getInternalPort());
    assertEquals(PortMapping.of(5000, 6000), job.getPorts().get("bar"));
    assertEquals("f00d", job.getEnv().get("BAD"));

    final String duplicateJob = control(
        "job", "create", name, version, image, "--", DO_NOTHING_COMMAND);
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
    long timeout = WAIT_TIMEOUT_SECONDS;
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

    JSONObject status = new JSONObject(control("host", "status", TEST_AGENT, "--json"));
    JSONObject env = (status.getJSONObject(TEST_AGENT)).getJSONObject("environment");
    assertEquals("PODNAME", env.getString("SPOTIFY_POD"));

    // Stop the agent
    agent.stopAsync().awaitTerminated();

    final Client client = Client.newBuilder()
        .setUser(TEST_USER)
        .setEndpoints(masterEndpoint)
        .build();

    awaitAgentStatus(client, TEST_AGENT, DOWN, WAIT_TIMEOUT_SECONDS, SECONDS);

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
    awaitAgentRegistered(client, agentName, WAIT_TIMEOUT_SECONDS, SECONDS);
    awaitAgentStatus(client, agentName, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Deploy the job on the agent
    final Deployment deployment = Deployment.of(jobId, START);
    final JobDeployResponse deployed = client.deploy(deployment, agentName).get();
    assertEquals(JobDeployResponse.Status.OK, deployed.getStatus());

    // Wait for the job to run
    final TaskStatus firstTaskStatus = awaitJobState(client, agentName, jobId, RUNNING,
                                                     LONG_WAIT_MINUTES, MINUTES);
    assertEquals(job, firstTaskStatus.getJob());

    // Stop the agent
    agent1.stopAsync().awaitTerminated();
    awaitAgentStatus(client, agentName, DOWN, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Start the agent again
    final AgentMain agent2 = startDefaultAgent(agentName);
    awaitAgentStatus(client, agentName, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Wait for a while and make sure that the same container is still running
    Thread.sleep(5000);
    final AgentStatus agentStatus = client.agentStatus(agentName).get();
    final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
    assertEquals(RUNNING, taskStatus.getState());
    assertEquals(firstTaskStatus.getContainerId(), taskStatus.getContainerId());

    // Stop the agent
    agent2.stopAsync().awaitTerminated();
    awaitAgentStatus(client, agentName, DOWN, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Kill the container
    dockerClient.stopContainer(firstTaskStatus.getContainerId());

    // Start the agent again
    final AgentMain agent3 = startDefaultAgent(agentName);
    awaitAgentStatus(client, agentName, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Wait for the job to be restarted in a new container
    final TaskStatus secondTaskStatus = await(LONG_WAIT_MINUTES, MINUTES,
                                              new Callable<TaskStatus>() {
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
    awaitAgentStatus(client, agentName, DOWN, WAIT_TIMEOUT_SECONDS, SECONDS);

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
    awaitAgentStatus(client, agentName, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Wait for the job to be restarted in a new container
    await(LONG_WAIT_MINUTES, MINUTES, new Callable<TaskStatus>() {
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
    awaitAgentStatus(client, agentName, DOWN, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Undeploy the container
    final JobUndeployResponse undeployed = client.undeploy(jobId, agentName).get();
    assertEquals(JobUndeployResponse.Status.OK, undeployed.getStatus());

    // Start the agent again
    startDefaultAgent(agentName);
    awaitAgentStatus(client, agentName, UP, WAIT_TIMEOUT_SECONDS, SECONDS);

    // Wait for the job to enter the STOPPED state
    awaitJobState(client, agentName, jobId, STOPPED, WAIT_TIMEOUT_SECONDS, SECONDS);
  }

  private String stopJob(final JobId jobId, final String agent) throws Exception {
    return control("job", "stop", jobId.toString(), agent);
  }

  private String deleteAgent(final String testAgent) throws Exception {
    return control("host", "delete", testAgent, "yes");
  }

  private JobId createJob(final String name, final String version, final String image,
                          final List<String> command) throws Exception {
    return createJob(name, version, image, command, EMPTY_ENV,
                     new HashMap<String, PortMapping>(), null);
  }

  private JobId createJob(final String name, final String version, final String image,
                          final List<String> command, final ImmutableMap<String, String> env)
      throws Exception {
    return createJob(name, version, image, command, env, new HashMap<String, PortMapping>(), null);
  }

  private JobId createJob(final String name, final String version, final String image,
                          final List<String> command, final Map<String, String> env,
                          final Map<String, PortMapping> ports) throws Exception {
    return createJob(name, version, image, command, env, ports, null);
  }

  private JobId createJob(final String name, final String version, final String image,
                          final List<String> command, final Map<String, String> env,
                          final Map<String, PortMapping> ports, final String namelessService)
      throws Exception {
    final List<String> args = Lists.newArrayList("-q", name, version, image);

    if (!env.isEmpty()) {
      args.add("--env");
      for (final Map.Entry<String, String> entry : env.entrySet()) {
        args.add(entry.getKey() + "=" + entry.getValue());
      }
    }

    if (!ports.isEmpty()) {
      args.add("--port");
      for (final Map.Entry<String, PortMapping> entry : ports.entrySet()) {
        String value = "" + entry.getValue().getInternalPort();
        if (entry.getValue().getExternalPort() != null) {
          value += ":" + entry.getValue().getExternalPort();
        }
        args.add(entry.getKey() + "=" + value);
      }
    }

    if (namelessService != null) {
      args.add("--service=" + namelessService);
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

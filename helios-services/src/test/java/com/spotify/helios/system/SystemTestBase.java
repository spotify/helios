/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kpelykh.docker.client.DockerClient;
import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.model.Container;
import com.kpelykh.docker.client.utils.LogReader;
import com.spotify.helios.PortAllocator;
import com.spotify.helios.ZooKeeperTestBase;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.cli.CliMain;
import com.spotify.helios.common.Client;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.common.protocol.JobStatus;
import com.spotify.helios.master.MasterMain;
import com.spotify.hermes.Hermes;
import com.sun.jersey.api.client.ClientResponse;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.lang.System.nanoTime;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

@RunWith(MockitoJUnitRunner.class)
public abstract class SystemTestBase extends ZooKeeperTestBase {

  static final Logger log = LoggerFactory.getLogger(SystemTestBase.class);

  static final String PREFIX = Long.toHexString(new SecureRandom().nextLong());

  static final int WAIT_TIMEOUT_SECONDS = 40;
  static final int LONG_WAIT_MINUTES = 2;

  static final int INTERNAL_PORT = 4444;

  // TODO (dano): use ephemeral port range when nameless is fixed
  final int EXTERNAL_PORT = new SecureRandom().nextInt(10000) + 30000;

  static final Map<String, String> EMPTY_ENV = emptyMap();
  static final Map<String, PortMapping> EMPTY_PORTS = emptyMap();
  static final Map<ServiceEndpoint, ServicePorts> EMPTY_REGISTRATION = emptyMap();

  static final JobId BOGUS_JOB = new JobId("bogus", "job", "badfood");
  static final String BOGUS_AGENT = "BOGUS_AGENT";

  static final String TEST_USER = "test-user";
  static final String TEST_AGENT = "test-agent";
  static final String TEST_MASTER = "test-master";

  static final List<String> DO_NOTHING_COMMAND = asList("sh", "-c", "while :; do sleep 1; done");

  static final TypeReference<Map<JobId, JobStatus>> STATUSES_TYPE =
      new TypeReference<Map<JobId, JobStatus>>() {};

  final int masterPort = PortAllocator.allocatePort("helios master");
  final String masterEndpoint = "tcp://localhost:" + masterPort;

  static final String DOCKER_ENDPOINT =
      fromNullable(getenv("DOCKER_ENDPOINT")).or("http://localhost:4160");

  final List<Service> services = newArrayList();
  final ExecutorService executorService = Executors.newCachedThreadPool();

  static final TypeReference<Map<String, Object>> OBJECT_TYPE =
      new TypeReference<Map<String, Object>>() {};

  Path agentStateDir;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule watcher = new TestWatcher() {
    protected void starting(Description description) {
      log.info(Strings.repeat("=", 80));
      log.info("STARTING: {}: {}", description.getClassName(), description.getMethodName());
      log.info(Strings.repeat("=", 80));
    }

    @Override
    protected void succeeded(final Description description) {
      log.info(Strings.repeat("=", 80));
      log.info("FINISHED: {}: {}", description.getClassName(), description.getMethodName());
      log.info(Strings.repeat("=", 80));
    }

    @Override
    protected void failed(final Throwable e, final Description description) {
      log.info(Strings.repeat("=", 80));
      log.info("FAILED  : {} {}", description.getClassName(), description.getMethodName());
      log.info(Strings.repeat("=", 80));
    }
  };

  static final String JOB_NAME = PREFIX + "foo";
  static final String JOB_VERSION = "17";

  final List<Client> clients = Lists.newArrayList();
  final List<com.spotify.hermes.service.Client> hermesClients = Lists.newArrayList();

  @Override
  @Before
  public void setUp() throws Exception {
    listThreads();
    super.setUp();
    ensure("/config");
    ensure("/status");
    agentStateDir = Files.createTempDirectory("helios-agent");
  }

  @Override
  @After
  public void teardown() throws Exception {
    for (final Client client : clients) {
      client.close();
    }
    clients.clear();

    for (com.spotify.hermes.service.Client client : hermesClients) {
      client.close();
    }
    hermesClients.clear();

    for (Service service : services) {
      try {
        service.stopAsync();
      } catch (Exception e) {
        log.error("Uncaught exception", e);
      }
    }
    for (Service service : services) {
      try {
        service.awaitTerminated();
      } catch (Exception e) {
        log.error("Service failed", e);
      }
    }
    services.clear();

    try {
      executorService.shutdownNow();
      executorService.awaitTermination(30, SECONDS);
    } catch (InterruptedException e) {
      log.error("Interrupted", e);
    }

    // Clean up docker
    try {
      final DockerClient dockerClient = new DockerClient(DOCKER_ENDPOINT);
      final List<Container> containers = dockerClient.listContainers(false);
      for (final Container container : containers) {
        for (final String name : container.names) {
          if (name.contains(PREFIX)) {
            try {
              dockerClient.kill(container.id);
            } catch (DockerException e) {
              e.printStackTrace();
            }
            break;
          }
        }
      }
    } catch (Exception e) {
      log.error("Docker client exception", e);
    }

    try {
      FileUtils.deleteDirectory(agentStateDir.toFile());
    } catch (IOException e) {
      log.error("Failed to remove agent state dir", e);
    }

    super.teardown();

    listThreads();
  }

  void listThreads() {
    final Set<Thread> threads = Thread.getAllStackTraces().keySet();
    final Map<String, Thread> sorted = Maps.newTreeMap();
    for (final Thread t : threads) {
      final ThreadGroup tg = t.getThreadGroup();
      if (t.isAlive() && (tg == null || !tg.getName().equals("system"))) {
        sorted.put(t.getName(), t);
      }
    }
    log.info("= THREADS " + Strings.repeat("=", 70));
    for (final Thread t : sorted.values()) {
      final ThreadGroup tg = t.getThreadGroup();
      log.info("{}: \"{}\" ({}{})", t.getId(), t.getName(),
               (tg == null ? "" : tg.getName() + " "),
               (t.isDaemon() ? "daemon" : ""));
    }
    log.info(Strings.repeat("=", 80));
  }

  void startDefaultMaster() throws Exception {
    startMaster("-vvvv",
                "--no-log-setup",
                "--munin-port", "0",
                "--hm", masterEndpoint,
                "--name", TEST_MASTER,
                "--zk", zookeeperEndpoint);
  }

  AgentMain startDefaultAgent(final String agentName, final String... args)
      throws Exception {
    final List<String> argsList = Lists.newArrayList("-vvvv",
                                                     "--no-log-setup",
                                                     "--munin-port", "0",
                                                     "--name", agentName,
                                                     "--docker", DOCKER_ENDPOINT,
                                                     "--zk", zookeeperEndpoint,
                                                     "--zk-session-timeout", "100",
                                                     "--zk-connection-timeout", "100",
                                                     "--state-dir", agentStateDir.toString());
    argsList.addAll(asList(args));
    return startAgent(argsList.toArray(new String[argsList.size()]));
  }

  MasterMain startMaster(final String... args) throws Exception {
    final MasterMain main = new MasterMain(args);
    main.startAsync();
    services.add(main);
    return main;
  }

  AgentMain startAgent(final String... args) throws Exception {
    final AgentMain main = new AgentMain(args);
    main.startAsync();
    services.add(main);
    return main;
  }

  JobId createJob(final String name, final String version, final String image,
                  final List<String> command) throws Exception {
    return createJob(name, version, image, command, EMPTY_ENV,
                     new HashMap<String, PortMapping>(), null);
  }

  JobId createJob(final String name, final String version, final String image,
                  final List<String> command, final ImmutableMap<String, String> env)
      throws Exception {
    return createJob(name, version, image, command, env, new HashMap<String, PortMapping>(), null);
  }

  JobId createJob(final String name, final String version, final String image,
                  final List<String> command, final Map<String, String> env,
                  final Map<String, PortMapping> ports) throws Exception {
    return createJob(name, version, image, command, env, ports, null);
  }

  JobId createJob(final String name, final String version, final String image,
                  final List<String> command, final Map<String, String> env,
                  final Map<String, PortMapping> ports,
                  final Map<ServiceEndpoint, ServicePorts> registration)
      throws Exception {
    checkArgument(name.contains(PREFIX), "Job name must contain PREFIX to enable cleanup");

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
        if (entry.getValue().getProtocol() != null) {
          value += "/" + entry.getValue().getProtocol();
        }
        args.add(entry.getKey() + "=" + value);
      }
    }

    if (registration != null) {
      for (final Map.Entry<ServiceEndpoint, ServicePorts> entry : registration.entrySet()) {
        final ServiceEndpoint r = entry.getKey();
        for (String portName : entry.getValue().getPorts().keySet()) {
          args.add("--register=" + ((r.getProtocol() == null)
                                    ? format("%s=%s", r.getName(), portName)
                                    : format("%s/%s=%s", r.getName(), r.getProtocol(), portName)));
        }
      }
    }

    args.add("--");
    args.addAll(command);

    final String createOutput = cli("job", "create", args);
    final String jobId = StringUtils.strip(createOutput);

    final String listOutput = cli("job", "list", "-q");
    assertContains(jobId, listOutput);
    return JobId.fromString(jobId);
  }

  void deployJob(final JobId jobId, final String agent)
      throws Exception {
    final String deployOutput = cli("job", "deploy", jobId.toString(), agent);
    assertContains(agent + ": done", deployOutput);

    final String listOutput = cli("host", "jobs", "-q", agent);
    assertContains(jobId.toString(), listOutput);
  }

  void undeployJob(final JobId jobId, final String host) throws Exception {
    final String bogusUndeployAgentWrong =
        cli("job", "undeploy", jobId.toString(), BOGUS_AGENT);
    assertContains("AGENT_NOT_FOUND", bogusUndeployAgentWrong);

    final String bogusUndeployJobWrong = cli("job", "undeploy", BOGUS_JOB.toString(), host);
    assertContains("Unknown job", bogusUndeployJobWrong);

    final String undeployOutput = cli("job", "undeploy", jobId.toString(), host);
    assertContains(host + ": done", undeployOutput);

    final String listOutput = cli("host", "jobs", "-q", host);
    assertNotContains(jobId.toString(), listOutput);
  }

  String startJob(final JobId jobId, final String agent) throws Exception {
    return cli("job", "start", jobId.toString(), agent);
  }

  String stopJob(final JobId jobId, final String agent) throws Exception {
    return cli("job", "stop", jobId.toString(), agent);
  }

  String deleteAgent(final String testAgent) throws Exception {
    return cli("host", "deregister", testAgent, "--force");
  }

  String cli(final String command, final String sub, final Object... args)
      throws Exception {
    return cli(command, sub, flatten(args));
  }

  String cli(final String command, final String sub, final String... args)
      throws Exception {
    return cli(command, sub, asList(args));
  }

  String cli(final String command, final String sub, final List<String> args)
      throws Exception {
    final List<String> commands = asList(command, sub, "-z", masterEndpoint, "--no-log-setup");
    final List<String> allArgs = newArrayList(concat(commands, args));
    return main(allArgs).toString();
  }

  ByteArrayOutputStream main(final String... args) throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    final CliMain main = new CliMain(new PrintStream(out), new PrintStream(err), args);
    main.run();
    return out;
  }

  ByteArrayOutputStream main(final Collection<String> args) throws Exception {
    return main(args.toArray(new String[args.size()]));
  }

  void awaitAgentRegistered(final String name, final long timeout, final TimeUnit timeUnit)
      throws Exception {
    await(timeout, timeUnit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final String output = cli("host", "list", "-q");
        return output.contains(name) ? true : null;
      }
    });
  }

  AgentStatus awaitAgentStatus(final String name, final AgentStatus.Status status,
                               final int timeout, final TimeUnit timeUnit) throws Exception {
    return await(timeout, timeUnit, new Callable<AgentStatus>() {
      @Override
      public AgentStatus call() throws Exception {
        final String output = cli("host", "status", name, "--json");
        final Map<String, AgentStatus> statuses = Json.read(
            output, new TypeReference<Map<String, AgentStatus>>() {});
        final AgentStatus agentStatus = statuses.get(name);
        if (agentStatus == null) {
          return null;
        }
        return (agentStatus.getStatus() == status) ? agentStatus : null;
      }
    });
  }

  TaskStatus awaitJobState(final Client client, final String slave,
                           final JobId jobId,
                           final TaskStatus.State state, final int timeout,
                           final TimeUnit timeunit) throws Exception {
    return await(timeout, timeunit, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final AgentStatus agentStatus = client.agentStatus(slave).get();
        final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
        return (taskStatus != null && taskStatus.getState() == state) ? taskStatus
                                                                      : null;
      }
    });
  }

  TaskStatus awaitJobThrottle(final Client client, final String slave,
                              final JobId jobId,
                              final ThrottleState throttled, final int timeout,
                              final TimeUnit timeunit) throws Exception {
    return await(timeout, timeunit, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final AgentStatus agentStatus = client.agentStatus(slave).get();
        final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
        return (taskStatus != null && taskStatus.getThrottled() == throttled) ? taskStatus : null;
      }
    });
  }

  void awaitAgentRegistered(final Client client, final String slave,
                            final int timeout,
                            final TimeUnit timeUnit) throws Exception {
    await(timeout, timeUnit, new Callable<AgentStatus>() {
      @Override
      public AgentStatus call() throws Exception {
        return client.agentStatus(slave).get();
      }
    });
  }

  AgentStatus awaitAgentStatus(final Client client, final String slave,
                               final AgentStatus.Status status,
                               final int timeout,
                               final TimeUnit timeUnit) throws Exception {
    return await(timeout, timeUnit, new Callable<AgentStatus>() {
      @Override
      public AgentStatus call() throws Exception {
        final AgentStatus agentStatus = client.agentStatus(slave).get();
        if (agentStatus == null) {
          return null;
        }
        return (agentStatus.getStatus() == status) ? agentStatus : null;
      }
    });
  }

  TaskStatus awaitTaskState(final JobId jobId, final String agent,
                            final TaskStatus.State state) throws Exception {
    long timeout = LONG_WAIT_MINUTES;
    TimeUnit timeUnit = MINUTES;
    return await(timeout, timeUnit, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final String output = cli("job", "status", "--json", jobId.toString());
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

  void awaitTaskGone(final Client client, final String host, final JobId jobId,
                     final long timeout, final TimeUnit timeunit) throws Exception {
    await(timeout, timeunit, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        final AgentStatus agentStatus = client.agentStatus(host).get();
        final TaskStatus taskStatus = agentStatus.getStatuses().get(jobId);
        return taskStatus == null ? true : null;
      }
    });
  }

  <T> T await(final long timeout, final TimeUnit timeUnit, final Callable<T> callable)
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

  String readLogFully(final ClientResponse logs) throws IOException {
    final LogReader logReader = new LogReader(logs.getEntityInputStream());
    StringBuilder stringBuilder = new StringBuilder();
    LogReader.Frame frame;
    while ((frame = logReader.readFrame()) != null) {
      stringBuilder.append(UTF_8.decode(frame.getBytes()));
    }
    logReader.close();
    return stringBuilder.toString();
  }

  List<Container> listContainers(final DockerClient dockerClient, final String needle) {
    final List<Container> containers = dockerClient.listContainers(false);
    final List<Container> matches = Lists.newArrayList();
    for (final Container container : containers) {
      for (final String name : container.names) {
        if (name.contains(needle)) {
          matches.add(container);
          break;
        }
      }
    }
    return matches;
  }

  void assertContains(String needle, String haystack) {
    assertThat(haystack, containsString(needle));
  }

  void assertNotContains(String needle, String haystack) {
    assertThat(haystack, not(containsString(needle)));
  }

  List<String> flatten(final Object... values) {
    final Iterable<Object> valuesList = asList(values);
    return flatten(valuesList);
  }

  List<String> flatten(final Iterable<?> values) {
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

  com.spotify.hermes.service.Client hermesClient(final String... endpoints) {
    final com.spotify.hermes.service.Client client = Hermes.newClient(endpoints);
    hermesClients.add(client);
    return client;
  }

  Client defaultClient() {
    return client(TEST_USER, masterEndpoint);
  }

  Client client(final String user, final String endpoint) {
    final Client client = Client.newBuilder()
        .setUser(user)
        .setEndpoints(endpoint)
        .build();
    clients.add(client);
    return client;
  }
}

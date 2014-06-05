/*
 * Copyright (c) 2014 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package com.spotify.helios.system;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.helios.Polling;
import com.spotify.helios.TemporaryPorts;
import com.spotify.helios.ZooKeeperStandaloneServerManager;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.agent.AgentMain;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.LogMessage;
import com.spotify.docker.client.LogReader;
import com.spotify.docker.client.messages.Container;
import com.spotify.helios.cli.CliMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.master.MasterMain;
import com.spotify.helios.servicescommon.coordination.Paths;
import com.sun.jersey.api.client.ClientResponse;

import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.CharMatcher.WHITESPACE;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.helios.common.descriptors.Goal.UNDEPLOY;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class SystemTestBase {

  private static final Logger log = LoggerFactory.getLogger(SystemTestBase.class);

  public static final int WAIT_TIMEOUT_SECONDS = 40;
  public static final int LONG_WAIT_MINUTES = 10;
  public static final int INTERNAL_PORT = 4444;
  public static final Map<String, String> EMPTY_ENV = emptyMap();
  public static final Map<String, PortMapping> EMPTY_PORTS = emptyMap();
  public static final Map<ServiceEndpoint, ServicePorts> EMPTY_REGISTRATION = emptyMap();
  public static final JobId BOGUS_JOB = new JobId("bogus", "job", Strings.repeat("0", 40));
  public static final String BOGUS_HOST = "BOGUS_HOST";
  public static final List<String> DO_NOTHING_COMMAND =
      asList("sh", "-c", "while :; do sleep 1; done");
  public static final String JOB_VERSION = "test_17";

  public static final int DOCKER_PORT = Integer.valueOf(env("DOCKER_PORT", "4160"));
  public static final String DOCKER_HOST = env("DOCKER_HOST", ":" + DOCKER_PORT);
  public static final String DOCKER_ADDRESS;
  public static final String DOCKER_ENDPOINT;

  static {
    // Parse DOCKER_HOST
    final String stripped = DOCKER_HOST.replaceAll(".*://", "");
    final HostAndPort hostAndPort = HostAndPort.fromString(stripped);
    final String host = hostAndPort.getHostText();
    final int port = hostAndPort.getPortOrDefault(DOCKER_PORT);
    DOCKER_ADDRESS = Strings.isNullOrEmpty(host) ? "localhost" : host;
    DOCKER_ENDPOINT = format("http://%s:%d", DOCKER_ADDRESS, port);
  }

  private static String env(final String key, final String defaultValue) {
    return fromNullable(getenv(key)).or(defaultValue);
  }

  private static final String TEST_USER = "test-user";
  private static final String TEST_HOST = "test-host";
  private static final String TEST_MASTER = "test-master";

  @Rule public final TemporaryPorts temporaryPorts = new TemporaryPorts();

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public final ExpectedException exception = ExpectedException.none();
  @Rule public final TestRule watcher = new LoggingTestWatcher();

  public final String PREFIX = "test_" + Integer.toHexString(new SecureRandom().nextInt());
  public final String JOB_NAME = PREFIX + "foo";

  private int masterPort;
  private int masterAdminPort;
  private String masterEndpoint;
  private boolean integrationMode;

  private final List<Service> services = newArrayList();
  private final ExecutorService executorService = Executors.newCachedThreadPool();
  private final List<HeliosClient> clients = Lists.newArrayList();

  private String testHost;
  private Path agentStateDirs;
  private String masterName;

  protected ZooKeeperTestManager zk;

  public boolean isIntegration() {
    return integrationMode;
  }

  public TemporaryPorts getTemporaryPorts() {
    return temporaryPorts;
  }

  public String getMasterEndpoint() {
    return masterEndpoint;
  }

  public ZooKeeperTestManager getZk() {
    return zk;
  }

  public String masterName() throws InterruptedException, ExecutionException {
    if (integrationMode) {
      if (masterName == null) {
        masterName = defaultClient().listMasters().get().get(0);
      }
      return masterName;
    } else {
      return "test-master";
    }
  }

  @BeforeClass
  public static void staticSetup() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  static void removeContainer(final DockerClient dockerClient, final String containerId)
      throws Exception {
    // Work around docker sometimes failing to remove a container directly after killing it
    Polling.await(1, MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          dockerClient.removeContainer(containerId);
          return true;
        } catch (DockerException e) {
          return null;
        }
      }
    });
    try {
      // This should fail with an exception if the container still exists
      dockerClient.inspectContainer(containerId);
      fail();
    } catch (DockerException ignore) {
    }
  }

  @Before
  public void baseSetup() throws Exception {
    masterPort = temporaryPorts.localPort("helios master");
    masterAdminPort = temporaryPorts.localPort("helios master admin");

    String className = getClass().getName();
    if (className.endsWith("ITCase")) {
      masterEndpoint = checkNotNull(System.getenv("HELIOS_ENDPOINT"),
                                    "For integration tests, HELIOS_ENDPOINT *must* be set");
      integrationMode = true;
    } else if (className.endsWith("Test") || className.endsWith("TestBase")) {
      integrationMode = false;
      masterEndpoint = "http://localhost:" + getMasterPort();
      // unit test
    } else {
      throw new RuntimeException("testClass neither ends in Test, TestBase or ITCase");
    }

    zk = zooKeeperTestManager();
    listThreads();
    zk.ensure("/config");
    zk.ensure("/status");
    agentStateDirs = temporaryFolder.newFolder("helios-agents").toPath();
  }

  protected ZooKeeperTestManager zooKeeperTestManager() {
    return new ZooKeeperStandaloneServerManager();
  }

  @After
  public void baseTeardown() throws Exception {
    tearDownJobs();
    for (final HeliosClient client : clients) {
      client.close();
    }
    clients.clear();

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
      final DockerClient dockerClient = new DefaultDockerClient(DOCKER_ENDPOINT);
      final List<Container> containers = dockerClient.listContainers();
      for (final Container container : containers) {
        for (final String name : container.names()) {
          if (name.contains(PREFIX)) {
            try {
              dockerClient.killContainer(container.id());
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

    zk.close();

    listThreads();
  }

  protected void tearDownJobs() throws InterruptedException, ExecutionException {
    if (!isIntegration()) {
      return;
    }

    if (System.getenv("ITCASE_PRESERVE_JOBS") != null) {
      return;
    }

    final List<ListenableFuture<JobUndeployResponse>> undeploys = Lists.newArrayList();
    final HeliosClient c = defaultClient();
    final Map<JobId, Job> jobs = c.jobs().get();
    for (JobId jobId : jobs.keySet()) {
      if (!jobId.toString().startsWith(PREFIX)) {
        continue;
      }
      final JobStatus st = c.jobStatus(jobId).get();
      final Set<String> hosts = st.getDeployments().keySet();
      for (String host : hosts) {
        log.info("Undeploying job " + jobId);
        undeploys.add(c.undeploy(jobId, host));
      }
    }
    Futures.allAsList(undeploys);

    final List<ListenableFuture<JobDeleteResponse>> deletes = Lists.newArrayList();
    for (JobId jobId : jobs.keySet()) {
      if (!jobId.toString().startsWith(PREFIX)) {
        continue;
      }
      log.info("Deleting job " + jobId);
      deletes.add(c.deleteJob(jobId));
    }
    Futures.allAsList(deletes);
  }

  private void listThreads() {
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

  protected void startDefaultMaster(String... args) throws Exception {
    if (isIntegration()) {
      checkArgument(args.length == 0,
                    "cannot start default master in integration test with arguments passed");
      return;
    }

    // TODO (dano): Move this bootstrapping to something reusable
    final CuratorFramework curator = zk.curator();
    curator.newNamespaceAwareEnsurePath(Paths.configHosts()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.configJobs()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.configJobRefs()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.statusHosts()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.statusMasters()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.historyJobs()).ensure(curator.getZookeeperClient());

    List<String> argsList = Lists.newArrayList("-vvvv",
                                               "--no-log-setup",
                                               "--http", getMasterEndpoint(),
                                               "--admin=" + getMasterAdminPort(),
                                               "--name", TEST_MASTER,
                                               "--zk", zk.connectString());
    argsList.addAll(asList(args));
    startMaster(argsList.toArray(new String[argsList.size()]));
    waitForMasterToConnectToZK();
  }

  protected void waitForMasterToConnectToZK() throws Exception {
    Polling.await(WAIT_TIMEOUT_SECONDS, SECONDS, new Callable<Object>() {
      @Override
      public Object call() {
        try {
          final List<String> masters = defaultClient().listMasters().get();
          return masters != null;
        } catch (Exception e) {
          return null;
        }
      }
    });
  }

  protected AgentMain startDefaultAgent(final String host, final String... args)
      throws Exception {
    if (isIntegration()) {
      checkArgument(args.length == 0,
                    "cannot start default agent in integration test with arguments passed");
      return null;
    }

    final Range<Integer> portRange = temporaryPorts.localPortRange("agent", 10);
    final String stateDir = agentStateDirs.resolve(host).toString();
    final List<String> argsList = Lists.newArrayList("-vvvv",
                                                     "--no-log-setup",
                                                     "--no-http",
                                                     "--name", host,
                                                     "--docker", DOCKER_ENDPOINT,
                                                     "--zk", zk.connectString(),
                                                     "--zk-session-timeout", "100",
                                                     "--zk-connection-timeout", "100",
                                                     "--state-dir", stateDir,
                                                     "--port-range=" +
                                                     portRange.lowerEndpoint() + ":" +
                                                     portRange.upperEndpoint());
    argsList.addAll(asList(args));
    return startAgent(argsList.toArray(new String[argsList.size()]));
  }

  MasterMain startMaster(final String... args) throws Exception {
    final MasterMain main = new MasterMain(args);
    main.startAsync().awaitRunning();
    services.add(main);
    return main;
  }

  AgentMain startAgent(final String... args) throws Exception {
    final AgentMain main = new AgentMain(args);
    main.startAsync().awaitRunning();
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

    for (final Map.Entry<String, PortMapping> entry : ports.entrySet()) {
      args.add("--port");
      String value = "" + entry.getValue().getInternalPort();
      if (entry.getValue().getExternalPort() != null) {
        value += ":" + entry.getValue().getExternalPort();
      }
      if (entry.getValue().getProtocol() != null) {
        value += "/" + entry.getValue().getProtocol();
      }
      args.add(entry.getKey() + "=" + value);
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

    final String createOutput = cli("create", args);
    final String jobId = WHITESPACE.trimFrom(createOutput);

    final String listOutput = cli("jobs", "-q");
    assertContains(jobId, listOutput);
    return JobId.fromString(jobId);
  }

  void deployJob(final JobId jobId, final String host)
      throws Exception {
    final String deployOutput = cli("deploy", jobId.toString(), host);
    assertContains(host + ": done", deployOutput);

    final String output = cli("status", "--host", host, "--json");
    final Map<JobId, JobStatus> statuses =
        Json.readUnchecked(output, new TypeReference<Map<JobId, JobStatus>>() {});
    assertTrue(statuses.keySet().contains(jobId));
  }

  void undeployJob(final JobId jobId, final String host) throws Exception {
    final String undeployOutput = cli("undeploy", jobId.toString(), host);
    assertContains(host + ": done", undeployOutput);

    final String output = cli("status", "--host", host, "--json");
    final Map<JobId, JobStatus> statuses =
        Json.readUnchecked(output, new TypeReference<Map<JobId, JobStatus>>() {});
    final JobStatus status = statuses.get(jobId);
    assertTrue(status == null ||
               status.getDeployments().get(host) == null ||
               status.getDeployments().get(host).getGoal() == UNDEPLOY);
  }

  String startJob(final JobId jobId, final String host) throws Exception {
    return cli("start", jobId.toString(), host);
  }

  String stopJob(final JobId jobId, final String host) throws Exception {
    return cli("stop", jobId.toString(), host);
  }

  String deregisterHost(final String host) throws Exception {
    return cli("deregister", host, "--force");
  }

  String cli(final String command, final Object... args)
      throws Exception {
    return cli(command, flatten(args));
  }

  String cli(final String command, final String... args)
      throws Exception {
    return cli(command, asList(args));
  }

  String cli(final String command, final List<String> args)
      throws Exception {
    final List<String> commands = asList(command, "-z", getMasterEndpoint(), "--no-log-setup");
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

  void awaitHostRegistered(final String name, final long timeout, final TimeUnit timeUnit)
      throws Exception {
    Polling.await(timeout, timeUnit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final String output = cli("hosts", "-q");
        return output.contains(name) ? true : null;
      }
    });
  }

  HostStatus awaitHostStatus(final String name, final HostStatus.Status status,
                             final int timeout, final TimeUnit timeUnit) throws Exception {
    return Polling.await(timeout, timeUnit, new Callable<HostStatus>() {
      @Override
      public HostStatus call() throws Exception {
        final String output = cli("hosts", name, "--json");
        final Map<String, HostStatus> statuses;
        try {
          statuses = Json.read(output, new TypeReference<Map<String, HostStatus>>() {});
        } catch (IOException e) {
          return null;
        }
        final HostStatus hostStatus = statuses.get(name);
        if (hostStatus == null) {
          return null;
        }
        return (hostStatus.getStatus() == status) ? hostStatus : null;
      }
    });
  }

  TaskStatus awaitJobState(final HeliosClient client, final String host,
                           final JobId jobId,
                           final TaskStatus.State state, final int timeout,
                           final TimeUnit timeunit) throws Exception {
    return Polling.await(timeout, timeunit, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final HostStatus hostStatus = getOrNull(client.hostStatus(host));
        if (hostStatus == null) {
          return null;
        }
        final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
        return (taskStatus != null && taskStatus.getState() == state) ? taskStatus
                                                                      : null;
      }
    });
  }

  TaskStatus awaitJobThrottle(final HeliosClient client, final String host,
                              final JobId jobId,
                              final ThrottleState throttled, final int timeout,
                              final TimeUnit timeunit) throws Exception {
    return Polling.await(timeout, timeunit, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final HostStatus hostStatus = getOrNull(client.hostStatus(host));
        if (hostStatus == null) {
          return null;
        }
        final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
        return (taskStatus != null && taskStatus.getThrottled() == throttled) ? taskStatus : null;
      }
    });
  }

  void awaitHostRegistered(final HeliosClient client, final String host,
                           final int timeout,
                           final TimeUnit timeUnit) throws Exception {
    Polling.await(timeout, timeUnit, new Callable<HostStatus>() {
      @Override
      public HostStatus call() throws Exception {
        return getOrNull(client.hostStatus(host));
      }
    });
  }

  HostStatus awaitHostStatus(final HeliosClient client, final String host,
                             final HostStatus.Status status,
                             final int timeout,
                             final TimeUnit timeUnit) throws Exception {
    return Polling.await(timeout, timeUnit, new Callable<HostStatus>() {
      @Override
      public HostStatus call() throws Exception {
        final HostStatus hostStatus = getOrNull(client.hostStatus(host));
        if (hostStatus == null) {
          return null;
        }
        return (hostStatus.getStatus() == status) ? hostStatus : null;
      }
    });
  }

  TaskStatus awaitTaskState(final JobId jobId, final String host,
                            final TaskStatus.State state) throws Exception {
    long timeout = LONG_WAIT_MINUTES;
    TimeUnit timeUnit = MINUTES;
    return Polling.await(timeout, timeUnit, new Callable<TaskStatus>() {
      @Override
      public TaskStatus call() throws Exception {
        final String output = cli("status", "--json", "--job", jobId.toString());
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
        final TaskStatus taskStatus = status.getTaskStatuses().get(host);
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

  void awaitTaskGone(final HeliosClient client, final String host, final JobId jobId,
                     final long timeout, final TimeUnit timeunit) throws Exception {
    Polling.await(timeout, timeunit, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        final HostStatus hostStatus = getOrNull(client.hostStatus(host));
        final TaskStatus taskStatus = hostStatus.getStatuses().get(jobId);
        final Deployment deployment = hostStatus.getJobs().get(jobId);
        return taskStatus == null && deployment == null ? true : null;
      }
    });
  }

  private <T> T getOrNull(final ListenableFuture<T> future)
      throws ExecutionException, InterruptedException {
    return Futures.withFallback(future, new FutureFallback<T>() {
      @Override
      public ListenableFuture<T> create(final Throwable t) throws Exception {
        return Futures.immediateFuture(null);
      }
    }).get();
  }

  String readLogFully(final ClientResponse logs) throws IOException {
    final LogReader logReader = new LogReader(logs.getEntityInputStream());
    StringBuilder stringBuilder = new StringBuilder();
    LogMessage logMessage;
    while ((logMessage = logReader.nextMessage()) != null) {
      stringBuilder.append(UTF_8.decode(logMessage.content()));
    }
    logReader.close();
    return stringBuilder.toString();
  }

  List<Container> listContainers(final DockerClient dockerClient, final String needle)
      throws DockerException, InterruptedException {
    final List<Container> containers = dockerClient.listContainers();
    final List<Container> matches = Lists.newArrayList();
    for (final Container container : containers) {
      if (container.names() != null) {
        for (final String name : container.names()) {
          if (name.contains(needle)) {
            matches.add(container);
            break;
          }
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

  protected HeliosClient defaultClient() {
    return client(TEST_USER, getMasterEndpoint());
  }

  HeliosClient client(final String user, final String endpoint) {
    final HeliosClient client = HeliosClient.newBuilder()
        .setUser(user)
        .setEndpoints(asList(URI.create(endpoint)))
        .build();
    clients.add(client);
    return client;
  }

  public int getMasterPort() {
    return masterPort;
  }

  int getMasterAdminPort() {
    return masterAdminPort;
  }

  public String getTestHost() throws InterruptedException, ExecutionException {
    if (integrationMode) {
      if (testHost == null) {
        final List<String> hosts = defaultClient().listHosts().get();
        testHost = hosts.get(new SecureRandom().nextInt(hosts.size()));
      }
      return testHost;
    } else {
      return TEST_HOST;
    }
  }
}

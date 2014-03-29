/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.system;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kpelykh.docker.client.DockerClient;
import com.kpelykh.docker.client.DockerException;
import com.kpelykh.docker.client.model.Container;
import com.kpelykh.docker.client.utils.LogReader;
import com.spotify.helios.Polling;
import com.spotify.helios.TemporaryPorts;
import com.spotify.helios.ZooKeeperStandaloneServerManager;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.cli.CliMain;
import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.HostStatus;
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
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Optional.fromNullable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.fail;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

public abstract class SystemTestBase {

  static final Logger log = LoggerFactory.getLogger(SystemTestBase.class);

  static final String PREFIX = Long.toHexString(new SecureRandom().nextLong());

  static final int WAIT_TIMEOUT_SECONDS = 40;
  static final int LONG_WAIT_MINUTES = 10;

  static final int INTERNAL_PORT = 4444;

  @Rule
  public TemporaryPorts temporaryPorts = new TemporaryPorts();

  // TODO (dano): Use temporaryPorts directly everywhere instead of these constants
  final int EXTERNAL_PORT1 = temporaryPorts.localPort("external-1");
  final int EXTERNAL_PORT2 = temporaryPorts.localPort("external-2");

  static final Map<String, String> EMPTY_ENV = emptyMap();
  static final Map<String, PortMapping> EMPTY_PORTS = emptyMap();
  static final Map<ServiceEndpoint, ServicePorts> EMPTY_REGISTRATION = emptyMap();

  static final JobId BOGUS_JOB = new JobId("bogus", "job", Strings.repeat("0", 40));
  static final String BOGUS_HOST = "BOGUS_HOST";

  static final String TEST_USER = "test-user";
  static final String TEST_HOST = "test-host";
  static final String TEST_MASTER = "test-master";

  static final List<String> DO_NOTHING_COMMAND = asList("sh", "-c", "while :; do sleep 1; done");

  static final TypeReference<Map<JobId, JobStatus>> STATUSES_TYPE =
      new TypeReference<Map<JobId, JobStatus>>() {};


  final int masterPort = temporaryPorts.localPort("helios master");
  final int masterAdminPort = temporaryPorts.localPort("helios master admin");
  protected final String masterEndpoint = "http://localhost:" + masterPort;

  static final String DOCKER_ENDPOINT =
      fromNullable(getenv("DOCKER_ENDPOINT"))
          .or(fromNullable(getenv("DOCKER_HOST")))
          .or("http://localhost:4160")
          .replace("tcp://", "http://");

  final List<Service> services = newArrayList();
  final ExecutorService executorService = Executors.newCachedThreadPool();

  static final TypeReference<Map<String, Object>> OBJECT_TYPE =
      new TypeReference<Map<String, Object>>() {};

  final AtomicInteger agentCounter = new AtomicInteger();
  Path agentStateDirs;

  protected ZooKeeperTestManager zk;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Rule
  public TestRule watcher = new TestWatcher() {
    @Override
    protected void starting(Description description) {
      if (Boolean.getBoolean("logToFile")) {
        final String name = description.getClassName() + "_" + description.getMethodName();
        setupFileLogging(name);
      }
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

  private void setupFileLogging(final String name) {
    final ch.qos.logback.classic.Logger rootLogger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME);
    final LoggerContext context = rootLogger.getLoggerContext();
    context.reset();
    final FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
    final String ts = new SimpleDateFormat("yyyyMMdd'T'HHmmss.SSS").format(new Date());
    final String pid = ManagementFactory.getRuntimeMXBean().getName().split("@", 2)[0];
    final Path directory = Paths.get(System.getProperty("logDir", "/tmp/helios-test/log/"));
    final String filename = String.format("%s-%s-%s.log", ts, name, pid);
    final Path file = directory.resolve(filename);
    final PatternLayoutEncoder ple = new PatternLayoutEncoder();
    ple.setContext(context);
    ple.setPattern("%d{HH:mm:ss.SSS} %-5level %logger{1} %F:%L - %msg%n");
    ple.start();
    fileAppender.setEncoder(ple);
    fileAppender.setFile(file.toString());
    fileAppender.setContext(context);
    fileAppender.start();
    rootLogger.setLevel(Level.DEBUG);
    rootLogger.addAppender(fileAppender);
    try {
      Files.createDirectories(directory);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    configureLogger("org.eclipse.jetty", Level.ERROR, false);
    configureLogger("org.apache.curator", Level.ERROR, false);
    configureLogger("org.apache.zookeeper", Level.ERROR, false);
    configureLogger("com.spotify.helios", Level.DEBUG, true);
  }

  private void configureLogger(final String name, final Level level, final boolean additive) {
    final ch.qos.logback.classic.Logger logger =
        (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(name);
    logger.setLevel(level);
    logger.setAdditive(additive);
  }

  static final String JOB_NAME = PREFIX + "foo";
  static final String JOB_VERSION = "17";

  final List<HeliosClient> clients = Lists.newArrayList();
  final List<com.spotify.hermes.service.Client> hermesClients = Lists.newArrayList();

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
    zk = zooKeeperTestManager();
    listThreads();
    zk.ensure("/config");
    zk.ensure("/status");
    agentStateDirs = Files.createTempDirectory("helios-agents");
  }

  protected ZooKeeperTestManager zooKeeperTestManager() {
    return new ZooKeeperStandaloneServerManager();
  }

  @After
  public void baseTeardown() throws Exception {
    for (final HeliosClient client : clients) {
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
      final DockerClient dockerClient = new DockerClient(DOCKER_ENDPOINT, false);
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

    FileUtils.deleteQuietly(agentStateDirs.toFile());

    zk.close();

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

  protected void startDefaultMaster() throws Exception {
    startMaster("-vvvv",
                "--no-log-setup",
                "--http", masterEndpoint,
                "--admin=" + masterAdminPort,
                "--name", TEST_MASTER,
                "--zk", zk.connectString());
  }

  AgentMain startDefaultAgent(final String host, final String... args)
      throws Exception {
    final String stateDir = agentStateDirs.resolve(host).toString();
    final List<String> argsList = Lists.newArrayList("-vvvv",
                                                     "--no-log-setup",
                                                     "--no-http",
                                                     "--name", host,
                                                     "--docker", DOCKER_ENDPOINT,
                                                     "--zk", zk.connectString(),
                                                     "--zk-session-timeout", "100",
                                                     "--zk-connection-timeout", "100",
                                                     "--state-dir", stateDir);
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

    final String createOutput = cli("job", "create", args);
    final String jobId = StringUtils.strip(createOutput);

    final String listOutput = cli("job", "list", "-q");
    assertContains(jobId, listOutput);
    return JobId.fromString(jobId);
  }

  void deployJob(final JobId jobId, final String host)
      throws Exception {
    final String deployOutput = cli("job", "deploy", jobId.toString(), host);
    assertContains(host + ": done", deployOutput);

    final String listOutput = cli("host", "jobs", "-q", host);
    assertContains(jobId.toString(), listOutput);
  }

  void undeployJob(final JobId jobId, final String host) throws Exception {
    final String undeployOutput = cli("job", "undeploy", jobId.toString(), host);
    assertContains(host + ": done", undeployOutput);

    final String listOutput = cli("host", "jobs", "-q", host);
    assertNotContains(jobId.toString(), listOutput);
  }

  String startJob(final JobId jobId, final String host) throws Exception {
    return cli("job", "start", jobId.toString(), host);
  }

  String stopJob(final JobId jobId, final String host) throws Exception {
    return cli("job", "stop", jobId.toString(), host);
  }

  String deregisterHost(final String host) throws Exception {
    return cli("host", "deregister", host, "--force");
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

  void awaitHostRegistered(final String name, final long timeout, final TimeUnit timeUnit)
      throws Exception {
    Polling.await(timeout, timeUnit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final String output = cli("host", "list", "-q");
        return output.contains(name) ? true : null;
      }
    });
  }

  HostStatus awaitHostStatus(final String name, final HostStatus.Status status,
                             final int timeout, final TimeUnit timeUnit) throws Exception {
    return Polling.await(timeout, timeUnit, new Callable<HostStatus>() {
      @Override
      public HostStatus call() throws Exception {
        final String output = cli("host", "status", name, "--json");
        final Map<String, HostStatus> statuses;
        try {
          statuses = Json.read(output, new TypeReference<Map<String, HostStatus>>() {
          });
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
        return taskStatus == null ? true : null;
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
      if (container.names != null) {
        for (final String name : container.names) {
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

  com.spotify.hermes.service.Client hermesClient(final String... endpoints) {
    final com.spotify.hermes.service.Client client = Hermes.newClient(endpoints);
    hermesClients.add(client);
    return client;
  }

  HeliosClient defaultClient() {
    return client(TEST_USER, masterEndpoint);
  }

  HeliosClient client(final String user, final String endpoint) {
    final HeliosClient client = HeliosClient.newBuilder()
        .setUser(user)
        .setEndpoints(asList(URI.create(endpoint)))
        .build();
    clients.add(client);
    return client;
  }
}

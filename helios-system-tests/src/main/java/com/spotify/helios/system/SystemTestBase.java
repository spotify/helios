/*-
 * -\-\-
 * Helios System Tests
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

package com.spotify.helios.system;

import static com.google.common.base.CharMatcher.WHITESPACE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.helios.cli.command.JobCreateCommand.DEFAULT_METADATA_ENVVARS;
import static com.spotify.helios.common.descriptors.DeploymentGroupStatus.State.FAILED;
import static com.spotify.helios.common.descriptors.Job.EMPTY_ENV;
import static com.spotify.helios.common.descriptors.Job.EMPTY_EXPIRES;
import static com.spotify.helios.common.descriptors.Job.EMPTY_GRACE_PERIOD;
import static com.spotify.helios.common.descriptors.Job.EMPTY_HOSTNAME;
import static com.spotify.helios.common.descriptors.Job.EMPTY_PORTS;
import static com.spotify.helios.common.descriptors.Job.EMPTY_REGISTRATION;
import static com.spotify.helios.common.descriptors.Job.EMPTY_VOLUMES;
import static java.lang.Integer.toHexString;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerHost;
import com.spotify.docker.client.exceptions.ContainerNotFoundException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.exceptions.DockerRequestException;
import com.spotify.docker.client.exceptions.ImageNotFoundException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.helios.Polling;
import com.spotify.helios.TemporaryPorts;
import com.spotify.helios.TemporaryPorts.AllocatedPort;
import com.spotify.helios.ZooKeeperTestManager;
import com.spotify.helios.ZooKeeperTestingServerManager;
import com.spotify.helios.agent.AgentMain;
import com.spotify.helios.cli.CliMain;
import com.spotify.helios.client.HeliosClient;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.DeploymentGroupStatus;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.Job.Builder;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;
import com.spotify.helios.master.MasterMain;
import com.spotify.helios.servicescommon.ZooKeeperAclProviders;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactory;
import com.spotify.helios.servicescommon.coordination.Paths;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
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

public abstract class SystemTestBase {

  private static final Logger log = LoggerFactory.getLogger(SystemTestBase.class);

  public static final int WAIT_TIMEOUT_SECONDS = 40;
  public static final int LONG_WAIT_SECONDS = 400;

  public static final String BUSYBOX = "spotify/busybox:latest";
  public static final String BUSYBOX_WITH_DIGEST =
      "busybox@sha256:16a2a52884c2a9481ed267c2d46483eac7693b813a63132368ab098a71303f8a";
  public static final String NGINX = "spotify/nginx-alpine:latest";
  public static final String UHTTPD = "spotify/docker-uhttpd:latest";
  public static final String ALPINE = "spotify/alpine:latest";
  public static final String MEMCACHED = "spotify/memcached-mini:latest";
  public static final List<String> IDLE_COMMAND = asList(
      "sh", "-c", "trap 'exit 0' SIGINT SIGTERM; while :; do sleep 1; done");

  public final String testTag = "test_" + randomHexString();
  public final String testJobName = "job_" + testTag;
  public final String testJobVersion = "v" + randomHexString();
  public final String testJobNameAndVersion = testJobName + ":" + testJobVersion;

  public static final DockerHost DOCKER_HOST = DockerHost.fromEnv();

  public static final String TEST_USER = "test-user";
  public static final String TEST_HOST = "test-host";
  public static final String TEST_MASTER = "test-master";

  public static final String MASTER_USER = "helios-master";
  public static final String MASTER_PASSWORD = "master-password";
  public static final String AGENT_USER = "helios-agent";
  public static final String AGENT_PASSWORD = "agent-password";
  public static final String MASTER_DIGEST =
      ZooKeeperAclProviders.digest(MASTER_USER, MASTER_PASSWORD);
  public static final String AGENT_DIGEST = ZooKeeperAclProviders.digest(
      AGENT_USER, AGENT_PASSWORD);

  @Rule public final TemporaryPorts temporaryPorts = TemporaryPorts.create();

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public final ExpectedException exception = ExpectedException.none();
  @Rule public final TestRule watcher = new LoggingTestWatcher();

  private int masterPort;
  private int masterAdminPort;
  private String masterEndpoint;
  private String masterAdminEndpoint;
  private Range<Integer> dockerPortRange;

  private final List<Service> services = newArrayList();
  private final List<HeliosClient> clients = Lists.newArrayList();

  private Path agentStateDirs;
  private Path masterStateDirs;

  private ZooKeeperTestManager zk;
  protected final String zkClusterId = String.valueOf(ThreadLocalRandom.current().nextInt(10000));

  // An HttpClient that can be used for sending arbitrary HTTP requests.
  protected CloseableHttpClient httpClient;

  @BeforeClass
  public static void staticSetup() {
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();
  }

  @Before
  public void baseSetup() throws Exception {
    System.setProperty("user.name", TEST_USER);
    masterPort = temporaryPorts.localPort("helios master");
    masterAdminPort = temporaryPorts.localPort("helios master admin");

    masterEndpoint = "http://localhost:" + masterPort();
    masterAdminEndpoint = "http://localhost:" + masterAdminPort();

    zk = zooKeeperTestManager();
    listThreads();
    zk.ensure("/config");
    zk.ensure("/status");
    agentStateDirs = temporaryFolder.newFolder("helios-agents").toPath();
    masterStateDirs = temporaryFolder.newFolder("helios-masters").toPath();

    // TODO (mbrown): not 100% sure what a minimal client is but it sounds good
    httpClient = HttpClients.createMinimal();
  }

  @Before
  public void dockerSetup() throws Exception {
    final String portRange = System.getenv("DOCKER_PORT_RANGE");

    final AllocatedPort allocatedPort;
    final int probePort;
    if (portRange != null) {
      final String[] parts = portRange.split(":", 2);
      dockerPortRange = Range.closedOpen(Integer.valueOf(parts[0]),
          Integer.valueOf(parts[1]));
      allocatedPort = Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<AllocatedPort>() {
        @Override
        public AllocatedPort call() throws Exception {
          final int port = ThreadLocalRandom.current().nextInt(dockerPortRange.lowerEndpoint(),
              dockerPortRange.upperEndpoint());
          return temporaryPorts.tryAcquire("docker-probe", port);
        }
      });
      probePort = allocatedPort.port();
    } else {
      dockerPortRange = temporaryPorts.localPortRange("docker", 10);
      probePort = dockerPortRange().lowerEndpoint();
      allocatedPort = null;
    }

    try {
      assertDockerReachable(probePort);
    } finally {
      if (allocatedPort != null) {
        allocatedPort.release();
      }
    }
  }

  protected DockerClient getNewDockerClient() throws Exception {
    if (isNullOrEmpty(DOCKER_HOST.dockerCertPath())) {
      return new DefaultDockerClient(DOCKER_HOST.uri());
    } else {
      final Path dockerCertPath = java.nio.file.Paths.get(DOCKER_HOST.dockerCertPath());
      return new DefaultDockerClient(DOCKER_HOST.uri(), new DockerCertificates(dockerCertPath));
    }
  }

  private void assertDockerReachable(final int probePort) throws Exception {
    try (final DockerClient docker = getNewDockerClient()) {
      // Pull our base images
      try {
        docker.inspectImage(BUSYBOX);
      } catch (ImageNotFoundException e) {
        docker.pull(BUSYBOX);
      }

      try {
        docker.inspectImage(ALPINE);
      } catch (ImageNotFoundException e) {
        docker.pull(ALPINE);
      }

      // Start a container with an exposed port
      final HostConfig hostConfig = HostConfig.builder()
          .portBindings(ImmutableMap.of("4711/tcp",
              singletonList(PortBinding.of("0.0.0.0", probePort))))
          .build();
      final ContainerConfig config = ContainerConfig.builder()
          .image(BUSYBOX)
          .cmd("nc", "-p", "4711", "-lle", "cat")
          .exposedPorts(ImmutableSet.of("4711/tcp"))
          .hostConfig(hostConfig)
          .build();
      final ContainerCreation creation = docker.createContainer(config, testTag + "-probe");
      final String containerId = creation.id();
      docker.startContainer(containerId);

      // Wait for container to come up
      Polling.await(5, SECONDS, new Callable<Object>() {
        @Override
        public Object call() throws Exception {
          final ContainerInfo info = docker.inspectContainer(containerId);
          return info.state().running() ? true : null;
        }
      });

      log.info("Verifying that docker containers are reachable");
      try {
        Polling.awaitUnchecked(5, SECONDS, new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            log.info("Probing: {}:{}", DOCKER_HOST.address(), probePort);
            try (final Socket ignored = new Socket(DOCKER_HOST.address(), probePort)) {
              return true;
            } catch (IOException e) {
              return false;
            }
          }
        });
      } catch (TimeoutException e) {
        fail("Please ensure that DOCKER_HOST is set to an address that where containers can "
             + "be reached. If docker is running in a local VM, DOCKER_HOST must be set to the "
             + "address of that VM. If docker can only be reached on a limited port range, "
             + "set the environment variable DOCKER_PORT_RANGE=start:end");
      }

      docker.killContainer(containerId);
    }
  }

  protected ZooKeeperTestManager zooKeeperTestManager() {
    return new ZooKeeperTestingServerManager();
  }

  @After
  public void baseTeardown() throws Exception {
    for (final HeliosClient client : clients) {
      client.close();
    }
    clients.clear();

    for (final Service service : services) {
      try {
        service.stopAsync();
      } catch (Exception e) {
        log.error("Uncaught exception", e);
      }
    }
    for (final Service service : services) {
      try {
        service.awaitTerminated();
      } catch (Exception e) {
        log.error("Service failed", e);
      }
    }
    services.clear();

    // Clean up docker
    try (final DockerClient dockerClient = getNewDockerClient()) {
      final List<Container> containers = dockerClient.listContainers();
      for (final Container container : containers) {
        for (final String name : container.names()) {
          if (name.contains(testTag)) {
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

    if (zk != null) {
      zk.close();
    }

    listThreads();
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

  protected TemporaryPorts temporaryPorts() {
    return temporaryPorts;
  }

  protected ZooKeeperTestManager zk() {
    return zk;
  }

  protected String masterEndpoint() {
    return masterEndpoint;
  }

  protected String masterAdminEndpoint() {
    return masterAdminEndpoint;
  }

  protected String masterName() throws InterruptedException, ExecutionException {
    return TEST_MASTER;
  }

  protected HeliosClient defaultClient() {
    return client(TEST_USER, masterEndpoint());
  }

  protected HeliosClient client(final String user, final String endpoint) {
    final HeliosClient client = HeliosClient.newBuilder()
        .setUser(user)
        .setEndpoints(singletonList(URI.create(endpoint)))
        .build();
    clients.add(client);
    return client;
  }

  protected int masterPort() {
    return masterPort;
  }

  protected int masterAdminPort() {
    return masterAdminPort;
  }

  public Range<Integer> dockerPortRange() {
    return dockerPortRange;
  }

  protected String testHost() throws InterruptedException, ExecutionException {
    return TEST_HOST;
  }

  protected List<String> setupDefaultMaster(String... args) throws Exception {
    return setupDefaultMaster(0, args);
  }

  protected List<String> setupDefaultMaster(final int offset, String... args) throws Exception {
    // TODO (dano): Move this bootstrapping to something reusable
    final CuratorFramework curator = zk.curatorWithSuperAuth();
    curator.newNamespaceAwareEnsurePath(Paths.configHosts()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.configJobs()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.configJobRefs()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.statusHosts()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.statusMasters()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.historyJobs()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.configId(zkClusterId))
        .ensure(curator.getZookeeperClient());

    final List<String> argsList = Lists.newArrayList(
        "-vvvv",
        "--no-log-setup",
        "--http", "http://0.0.0.0:" + (masterPort() + offset),
        "--admin", "http://0.0.0.0:" + (masterAdminPort() + offset),
        "--domain", "",
        "--zk", zk.connectString(),
        "--zk-enable-acls",
        "--zk-acl-agent-user", AGENT_USER,
        "--zk-acl-agent-digest", AGENT_DIGEST,
        "--zk-acl-master-user", MASTER_USER,
        "--zk-acl-master-password", MASTER_PASSWORD
    );

    final String name;
    if (asList(args).contains("--name")) {
      name = args[asList(args).indexOf("--name") + 1];
    } else {
      name = TEST_MASTER + offset;
      argsList.addAll(asList("--name", TEST_MASTER));
    }

    final String stateDir = masterStateDirs.resolve(name).toString();
    argsList.addAll(asList("--state-dir", stateDir));

    argsList.addAll(asList(args));

    return argsList;
  }

  protected MasterMain startDefaultMaster(String... args) throws Exception {
    return startDefaultMaster(0, args);
  }

  protected MasterMain startDefaultMaster(final int offset,
                                          final String... args) throws Exception {
    final List<String> argsList = setupDefaultMaster(offset, args);

    if (argsList == null) {
      return null;
    }

    final MasterMain master = startMaster(argsList.toArray(new String[argsList.size()]));

    waitForMasterToBeFullyUp();

    return master;
  }

  protected Map<String, MasterMain> startDefaultMasters(final int numMasters, String... args)
      throws Exception {
    final Map<String, MasterMain> masters = Maps.newHashMap();

    for (int i = 0; i < numMasters; i++) {
      final String name = TEST_MASTER + i;
      final List<String> argsList = Lists.newArrayList(args);
      argsList.addAll(asList("--name", name));
      masters.put(name, startDefaultMaster(i, argsList.toArray(new String[argsList.size()])));
    }

    return masters;
  }

  protected void waitForMasterToBeFullyUp() throws Exception {
    log.debug("waitForMasterToBeFullyUp: beginning wait loop");
    Polling.await(WAIT_TIMEOUT_SECONDS, SECONDS, new Callable<Object>() {
      @Override
      public Object call() {
        try {
          // While MasterService will start listening for http requests on the main and admin ports
          // as soon as it is started (without waiting for ZK to be available), the Healthcheck
          // registered for Zookeeper connectivity will cause the HealthcheckServlet to not return
          // 200 OK until ZK is connected to (and even better, until *everything* is healthy).
          final HttpGet request = new HttpGet(masterAdminEndpoint + "/healthcheck");

          try (CloseableHttpResponse response = httpClient.execute(request)) {
            final int status = response.getStatusLine().getStatusCode();
            log.debug("waitForMasterToBeFullyUp: healthcheck endpoint returned {}", status);
            return status == HttpStatus.SC_OK;
          }
        } catch (Exception e) {
          return null;
        }
      }
    });
  }

  protected void startDefaultMasterDontWaitForZk(final CuratorClientFactory curatorClientFactory,
                                                 final String... args) throws Exception {
    final List<String> argsList = setupDefaultMaster(args);

    if (argsList == null) {
      return;
    }

    startMaster(curatorClientFactory, argsList.toArray(new String[argsList.size()]));
  }

  protected AgentMain startDefaultAgent(final String host, final String... args)
      throws Exception {
    final String stateDir = agentStateDirs.resolve(host).toString();
    final List<String> argsList = Lists.newArrayList(
        "-vvvv",
        "--no-log-setup",
        "--no-http",
        "--name", host,
        "--docker=" + DOCKER_HOST.host(),
        "--zk", zk.connectString(),
        "--zk-session-timeout", "100",
        "--zk-connection-timeout", "100",
        "--zk-enable-acls",
        "--zk-acl-master-user", MASTER_USER,
        "--zk-acl-master-digest", MASTER_DIGEST,
        "--zk-acl-agent-user", AGENT_USER,
        "--zk-acl-agent-password", AGENT_PASSWORD,
        "--state-dir", stateDir,
        "--domain", "",
        "--port-range="
        + dockerPortRange.lowerEndpoint() + ":"
        + dockerPortRange.upperEndpoint()
    );
    argsList.addAll(asList(args));
    return startAgent(argsList.toArray(new String[argsList.size()]));
  }

  protected MasterMain startMaster(final String... args) throws Exception {
    final MasterMain main = new MasterMain(args);
    main.startAsync().awaitRunning();
    services.add(main);
    return main;
  }

  MasterMain startMaster(final CuratorClientFactory curatorClientFactory,
                         final String... args) throws Exception {
    final MasterMain main = new MasterMain(curatorClientFactory, args);
    main.startAsync().awaitRunning();
    services.add(main);
    return main;
  }

  protected AgentMain startAgent(final String... args) throws Exception {
    final AgentMain main = new AgentMain(args);
    main.startAsync().awaitRunning();
    services.add(main);
    return main;
  }

  protected void stopAgent(final AgentMain main) throws Exception {
    main.stopAsync().awaitTerminated();
    services.remove(main);
  }

  protected JobId createJob(final String name,
                            final String version,
                            final String image,
                            final List<String> command) throws Exception {
    return createJob(name, version, image, command, EMPTY_ENV, EMPTY_PORTS, EMPTY_REGISTRATION);
  }

  protected JobId createJob(final String name,
                            final String version,
                            final String image,
                            final List<String> command,
                            final Date expires) throws Exception {
    return createJob(name, version, image, EMPTY_HOSTNAME, command, EMPTY_ENV, EMPTY_PORTS,
        EMPTY_REGISTRATION, EMPTY_GRACE_PERIOD, EMPTY_VOLUMES, expires);
  }

  protected JobId createJob(final String name,
                            final String version,
                            final String image,
                            final List<String> command,
                            final ImmutableMap<String, String> env)
      throws Exception {
    return createJob(name, version, image, command, env, EMPTY_PORTS, EMPTY_REGISTRATION);
  }

  protected JobId createJob(final String name,
                            final String version,
                            final String image,
                            final List<String> command,
                            final Map<String, String> env,
                            final Map<String, PortMapping> ports) throws Exception {
    return createJob(name, version, image, command, env, ports, EMPTY_REGISTRATION);
  }

  protected JobId createJob(final String name,
                            final String version,
                            final String image,
                            final List<String> command,
                            final Map<String, String> env,
                            final Map<String, PortMapping> ports,
                            final Map<ServiceEndpoint, ServicePorts> registration)
      throws Exception {
    return createJob(name, version, image, command, env, ports, registration, EMPTY_GRACE_PERIOD,
        EMPTY_VOLUMES);
  }

  protected JobId createJob(final String name,
                            final String version,
                            final String image,
                            final List<String> command,
                            final Map<String, String> env,
                            final Map<String, PortMapping> ports,
                            final Map<ServiceEndpoint, ServicePorts> registration,
                            final Integer gracePeriod,
                            final Map<String, String> volumes) throws Exception {
    return createJob(name, version, image, EMPTY_HOSTNAME, command, env, ports, registration,
        gracePeriod, volumes, EMPTY_EXPIRES);
  }

  protected JobId createJob(final String name,
                            final String version,
                            final String image,
                            final String hostname,
                            final List<String> command,
                            final Map<String, String> env,
                            final Map<String, PortMapping> ports,
                            final Map<ServiceEndpoint, ServicePorts> registration,
                            final Integer gracePeriod,
                            final Map<String, String> volumes,
                            final Date expires) throws Exception {
    return createJob(Job.newBuilder()
        .setName(name)
        .setVersion(version)
        .setImage(image)
        .setHostname(hostname)
        .setCommand(command)
        .setEnv(env)
        .setPorts(ports)
        .setRegistration(registration)
        .setGracePeriod(gracePeriod)
        .setVolumes(volumes)
        .setExpires(expires)
        .build());
  }

  protected JobId createJob(final Job job) throws Exception {
    final String createOutput = createJobRawOutput(job);
    final String jobId = WHITESPACE.trimFrom(createOutput);

    return JobId.fromString(jobId);
  }

  protected String createJobRawOutput(final Job job) throws Exception {
    final String name = job.getId().getName();
    checkArgument(name.contains(testTag), "Job name must contain testTag to enable cleanup");

    final String serializedConfig = Json.asNormalizedString(job);
    final File configFile = temporaryFolder.newFile();
    Files.write(serializedConfig, configFile, Charsets.UTF_8);

    final List<String> args = ImmutableList.of("-q", "-f", configFile.getAbsolutePath());
    return cli("create", args);
  }

  protected void deployJob(final JobId jobId, final String host) throws Exception {
    deployJob(jobId, host, null);
  }

  protected void deployJob(final JobId jobId, final String host, final String token)
      throws Exception {
    final List<String> deployArgs = Lists.newArrayList(jobId.toString(), host);

    if (token != null) {
      deployArgs.addAll(ImmutableList.of("--token", token));
    }

    final String deployOutput = cli("deploy", deployArgs);
    assertThat(deployOutput, containsString(host + ": done"));

    final String output = cli("status", "--host", host, "--json");
    final Map<JobId, JobStatus> statuses =
        Json.readUnchecked(output, new TypeReference<Map<JobId, JobStatus>>() {
        });
    assertTrue(statuses.keySet().contains(jobId));
  }

  protected void undeployJob(final JobId jobId, final String host) throws Exception {
    final String undeployOutput = cli("undeploy", jobId.toString(), host);
    assertThat(undeployOutput, containsString(host + ": done"));

    final String output = cli("status", "--host", host, "--json");
    final Map<JobId, JobStatus> statuses =
        Json.readUnchecked(output, new TypeReference<Map<JobId, JobStatus>>() {
        });
    final JobStatus status = statuses.get(jobId);
    assertTrue(status == null || status.getDeployments().get(host) == null);
  }

  protected String startJob(final JobId jobId, final String host) throws Exception {
    return cli("start", jobId.toString(), host);
  }

  protected String stopJob(final JobId jobId, final String host) throws Exception {
    return cli("stop", jobId.toString(), host);
  }

  protected String deregisterHost(final String host) throws Exception {
    return cli("deregister", host, "--yes");
  }

  protected String cli(final String command, final Object... args)
      throws Exception {
    return cli(command, flatten(args));
  }

  protected String cli(final String command, final String... args)
      throws Exception {
    return cli(command, asList(args));
  }

  protected String cli(final String command, final List<String> args)
      throws Exception {
    final List<String> commands = asList(command, "-z", masterEndpoint(), "--no-log-setup");
    final List<String> allArgs = newArrayList(concat(commands, args));
    return main(allArgs).toString();
  }

  protected <T> T cliJson(final Class<T> klass, final String command, final String... args)
      throws Exception {
    return cliJson(klass, command, asList(args));
  }

  protected <T> T cliJson(final Class<T> klass, final String command, final List<String> args)
      throws Exception {
    final List<String> args0 = newArrayList("--json");
    args0.addAll(args);
    return Json.read(cli(command, args0), klass);
  }

  protected ByteArrayOutputStream main(final String... args) throws Exception {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final ByteArrayOutputStream err = new ByteArrayOutputStream();
    final CliMain main = new CliMain(new PrintStream(out), new PrintStream(err), args);
    main.run();
    return out;
  }

  protected ByteArrayOutputStream main(final Collection<String> args) throws Exception {
    return main(args.toArray(new String[args.size()]));
  }

  protected void awaitHostRegistered(final String name, final long timeout, final TimeUnit timeUnit)
      throws Exception {
    Polling.await(timeout, timeUnit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final String output = cli("hosts", "-q");
        return output.contains(name) ? true : null;
      }
    });
  }

  protected void awaitHostRegistered(final HeliosClient client,
                                     final String host,
                                     final int timeout,
                                     final TimeUnit timeUnit) throws Exception {
    Polling.await(timeout, timeUnit, new Callable<HostStatus>() {
      @Override
      public HostStatus call() throws Exception {
        return getOrNull(client.hostStatus(host));
      }
    });
  }

  protected HostStatus awaitHostStatus(final HeliosClient client,
                                       final String host,
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

  protected HostStatus awaitHostStatus(final String name, final HostStatus.Status status,
                                       final int timeout, final TimeUnit timeUnit)
      throws Exception {
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

  protected TaskStatus awaitJobState(final HeliosClient client, final String host,
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

  protected TaskStatus awaitJobThrottle(final HeliosClient client, final String host,
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

  protected HostStatus awaitHostStatusWithLabels(final HeliosClient client,
                                                 final String host,
                                                 final HostStatus.Status status,
                                                 final Map<String, String> labels)
      throws Exception {

    final HostStatus hostStatus = Polling.await(LONG_WAIT_SECONDS, SECONDS, () -> {
      final HostStatus candidate = getOrNull(client.hostStatus(host));

      if (candidate == null || candidate.getStatus() != status
          // labels are stored in ZK after the host has come up
          || candidate.getLabels().size() != labels.size()) {

        return null;
      }
      return candidate;
    });

    assertThat("host " + host + " has status=" + status + " with labels=" + hostStatus.getLabels(),
        hostStatus.getLabels(), is(labels));

    return hostStatus;
  }

  protected HostStatus awaitHostStatusWithHostInfo(final HeliosClient client, final String host,
                                                   final HostStatus.Status status,
                                                   final int timeout,
                                                   final TimeUnit timeUnit) throws Exception {
    return Polling.await(timeout, timeUnit, new Callable<HostStatus>() {
      @Override
      public HostStatus call() throws Exception {
        final HostStatus hostStatus = getOrNull(client.hostStatus(host));
        if (hostStatus == null || hostStatus.getHostInfo() == null) {
          return null;
        }
        return (hostStatus.getStatus() == status) ? hostStatus : null;
      }
    });
  }

  protected TaskStatus awaitTaskState(final JobId jobId, final String host,
                                      final TaskStatus.State state) throws Exception {
    return Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<TaskStatus>() {
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

  protected void awaitTaskGone(final HeliosClient client, final String host, final JobId jobId,
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

  protected DeploymentGroupStatus awaitDeploymentGroupStatus(
      final HeliosClient client,
      final String name,
      final DeploymentGroupStatus.State expected)
      throws Exception {
    return Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<DeploymentGroupStatus>() {
      @Override
      public DeploymentGroupStatus call() throws Exception {
        final DeploymentGroupStatusResponse response = getOrNull(
            client.deploymentGroupStatus(name));

        if (response != null) {
          final DeploymentGroupStatus status = response.getDeploymentGroupStatus();
          final DeploymentGroupStatus.State actual = status.getState();
          // The deployment group failed when we did not expect it to.
          if (actual == FAILED && actual != expected) {
            throw new AssertionError("Deployment group " + name + " failed unexpectedly: "
                                     + status.getError());
          }
          // The deployment group reached our desired status.
          if (actual == expected) {
            return status;
          }
        }

        return null;
      }
    });
  }

  protected <T> T getOrNull(final ListenableFuture<T> future)
      throws ExecutionException, InterruptedException {
    return Futures.catching(future, Exception.class, new Function<Exception, T>() {
      @Override
      public T apply(final Exception ex) {
        return null;
      }
    }).get();
  }

  protected static void removeContainer(final DockerClient dockerClient, final String containerId)
      throws Exception {
    // Work around docker sometimes failing to remove a container directly after killing it
    Polling.await(1, MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          dockerClient.killContainer(containerId);
        } catch (DockerRequestException e) {
          if (e.message().contains("is not running")) {
            // Container already isn't running. So we continue.
          } else {
            throw e;
          }
        }

        try {
          dockerClient.removeContainer(containerId);
          return true;
        } catch (ContainerNotFoundException e) {
          // We're done here
          return true;
        } catch (DockerException e) {
          if ((e instanceof DockerRequestException)
              && ((DockerRequestException) e).message().contains(
              "Driver btrfs failed to remove root filesystem")) {
            // Workaround btrfs issue where removing containers throws an exception,
            // but succeeds anyway.
            return true;
          } else {
            return null;
          }
        }
      }
    });
  }

  protected List<Container> listContainers(final DockerClient dockerClient, final String needle)
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

  protected List<String> flatten(final Object... values) {
    final Iterable<Object> valuesList = asList(values);
    return flatten(valuesList);
  }

  protected List<String> flatten(final Iterable<?> values) {
    final List<String> list = new ArrayList<>();
    for (final Object value : values) {
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

  protected void assertJobsEqual(final Map<JobId, Job> expected, final Map<JobId, Job> actual) {
    assertEquals(expected.size(), actual.size());
    for (final Map.Entry<JobId, Job> entry : actual.entrySet()) {
      assertJobEquals(expected.get(entry.getKey()), entry.getValue());
    }
  }

  protected void assertJobEquals(final Job expected, final Job actual) {
    final Builder expectedBuilder = expected.toBuilder();

    // hack to make sure that any environment variables that were folded into the created job
    // because of environment variables set at runtime on the test-running-agent are removed
    // from the actual when we assert the equality below
    final Builder actualBuilder = actual.toBuilder();
    final Map<String, String> metadata = Maps.newHashMap(actual.getMetadata());
    for (final Map.Entry<String, String> entry : DEFAULT_METADATA_ENVVARS.entrySet()) {
      final String envVar = entry.getKey();
      final String metadataKey = entry.getValue();
      final String envValue = System.getenv(envVar);
      if (envValue != null
          && actual.getMetadata().containsKey(metadataKey)
          && actual.getMetadata().get(metadataKey).equals(envValue)) {
        metadata.remove(metadataKey);
      }
    }
    actualBuilder.setMetadata(metadata);

    // Remove created timestamp set by master
    actualBuilder.setCreated(null);

    // copy the hash
    expectedBuilder.setHash(actualBuilder.build().getId().getHash());

    assertEquals(expectedBuilder.build(), actualBuilder.build());
  }

  protected static String randomHexString() {
    return toHexString(ThreadLocalRandom.current().nextInt());
  }

  protected void resetAgentStateDir() throws IOException {
    agentStateDirs = temporaryFolder.newFolder(UUID.randomUUID().toString()).toPath();
  }

  protected static boolean isCircleCi() {
    final String env = System.getenv("CIRCLECI");
    return env != null && "true".equalsIgnoreCase(env);
  }
}

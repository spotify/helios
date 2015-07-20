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

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.io.Files;
import com.google.common.util.concurrent.FutureFallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.spotify.docker.client.ContainerNotFoundException;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.DockerRequestException;
import com.spotify.docker.client.ImageNotFoundException;
import com.spotify.docker.client.LogMessage;
import com.spotify.docker.client.LogReader;
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
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.descriptors.PortMapping;
import com.spotify.helios.common.descriptors.ServiceEndpoint;
import com.spotify.helios.common.descriptors.ServicePorts;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.descriptors.ThrottleState;
import com.spotify.helios.common.protocol.DeploymentGroupStatusResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.master.MasterMain;
import com.spotify.helios.servicescommon.DockerHost;
import com.spotify.helios.servicescommon.coordination.CuratorClientFactory;
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
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.net.URI;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.CharMatcher.WHITESPACE;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Lists.newArrayList;
import static com.spotify.helios.common.descriptors.Job.EMPTY_ENV;
import static com.spotify.helios.common.descriptors.Job.EMPTY_EXPIRES;
import static com.spotify.helios.common.descriptors.Job.EMPTY_GRACE_PERIOD;
import static com.spotify.helios.common.descriptors.Job.EMPTY_PORTS;
import static com.spotify.helios.common.descriptors.Job.EMPTY_REGISTRATION;
import static com.spotify.helios.common.descriptors.Job.EMPTY_VOLUMES;
import static com.spotify.helios.common.descriptors.Job.EMPTY_HOSTNAME;
import static java.lang.Integer.toHexString;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class SystemTestBase {

  private static final Logger log = LoggerFactory.getLogger(SystemTestBase.class);

  public static final int WAIT_TIMEOUT_SECONDS = 40;
  public static final int LONG_WAIT_SECONDS = 400;

  public static final String BUSYBOX = "busybox:latest";
  public static final String NGINX = "rohan/nginx-alpine:latest";
  public static final String UHTTPD = "fnichol/docker-uhttpd:latest";
  public static final String ALPINE = "onescience/alpine:latest";
  public static final String MEMCACHED = "rohan/memcached-mini:latest";
  public static final List<String> IDLE_COMMAND = asList(
      "sh", "-c", "trap 'exit 0' SIGINT SIGTERM; while :; do sleep 1; done");

  public final String testTag = "test_" + toHexString(ThreadLocalRandom.current().nextInt());
  public final String testJobName = "job_" + testTag;
  public final String testJobVersion = "v" + toHexString(ThreadLocalRandom.current().nextInt());
  public final String testJobNameAndVersion = testJobName + ":" + testJobVersion;

  public static final DockerHost DOCKER_HOST = DockerHost.fromEnv();

  public static final String TEST_USER = "test-user";
  public static final String TEST_HOST = "test-host";
  public static final String TEST_MASTER = "test-master";

  @Rule public final TemporaryPorts temporaryPorts = TemporaryPorts.create();

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public final ExpectedException exception = ExpectedException.none();
  @Rule public final TestRule watcher = new LoggingTestWatcher();

  private int masterPort;
  private int masterAdminPort;
  private String masterEndpoint;
  private boolean integrationMode;
  private Range<Integer> dockerPortRange;

  private final List<Service> services = newArrayList();
  private final List<HeliosClient> clients = Lists.newArrayList();

  private String testHost;
  private Path agentStateDirs;
  private String masterName;

  private ZooKeeperTestManager zk;
  protected static String zooKeeperNamespace = null;
  protected final String zkClusterId = String.valueOf(ThreadLocalRandom.current().nextInt(10000));

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

    String className = getClass().getName();
    if (className.endsWith("ITCase")) {
      masterEndpoint = checkNotNull(System.getenv("HELIOS_ENDPOINT"),
                                    "For integration tests, HELIOS_ENDPOINT *must* be set");
      integrationMode = true;
    } else if (className.endsWith("Test")) {
      integrationMode = false;
      masterEndpoint = "http://localhost:" + masterPort();
      // unit test
    } else {
      throw new RuntimeException("Test class' name must end in either 'Test' or 'ITCase'.");
    }

    zk = zooKeeperTestManager();
    listThreads();
    zk.ensure("/config");
    zk.ensure("/status");
    agentStateDirs = temporaryFolder.newFolder("helios-agents").toPath();
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
      final ContainerConfig config = ContainerConfig.builder()
          .image(BUSYBOX)
          .cmd("nc", "-p", "4711", "-lle", "cat")
          .exposedPorts(ImmutableSet.of("4711/tcp"))
          .build();
      final HostConfig hostConfig = HostConfig.builder()
          .portBindings(ImmutableMap.of("4711/tcp",
                                        asList(PortBinding.of("0.0.0.0", probePort))))
          .build();
      final ContainerCreation creation = docker.createContainer(config, testTag + "-probe");
      final String containerId = creation.id();
      docker.startContainer(containerId, hostConfig);

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
        fail("Please ensure that DOCKER_HOST is set to an address that where containers can " +
             "be reached. If docker is running in a local VM, DOCKER_HOST must be set to the " +
             "address of that VM. If docker can only be reached on a limited port range, " +
             "set the environment variable DOCKER_PORT_RANGE=start:end");
      }

      docker.killContainer(containerId);
    }
  }

  protected ZooKeeperTestManager zooKeeperTestManager() {
    return new ZooKeeperTestingServerManager(zooKeeperNamespace);
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
      if (!jobId.toString().startsWith(testTag)) {
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
      if (!jobId.toString().startsWith(testTag)) {
        continue;
      }
      log.info("Deleting job " + jobId);
      deletes.add(c.deleteJob(jobId));
    }
    Futures.allAsList(deletes);
  }

  protected boolean isIntegration() {
    return integrationMode;
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

  protected String masterName() throws InterruptedException, ExecutionException {
    if (integrationMode) {
      if (masterName == null) {
        masterName = defaultClient().listMasters().get().get(0);
      }
      return masterName;
    } else {
      return "test-master";
    }
  }

  protected HeliosClient defaultClient() {
    return client(TEST_USER, masterEndpoint());
  }

  protected HeliosClient client(final String user, final String endpoint) {
    final HeliosClient client = HeliosClient.newBuilder()
        .setUser(user)
        .setEndpoints(asList(URI.create(endpoint)))
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

  protected List<String> setupDefaultMaster(String... args) throws Exception {
    if (isIntegration()) {
      checkArgument(args.length == 0,
                    "cannot start default master in integration test with arguments passed");
      return null;
    }

    // TODO (dano): Move this bootstrapping to something reusable
    final CuratorFramework curator = zk.curator();
    curator.newNamespaceAwareEnsurePath(Paths.configHosts()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.configJobs()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.configJobRefs()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.statusHosts()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.statusMasters()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.historyJobs()).ensure(curator.getZookeeperClient());
    curator.newNamespaceAwareEnsurePath(Paths.configId(zkClusterId))
        .ensure(curator.getZookeeperClient());

    final List<String> argsList = Lists.newArrayList("-vvvv",
                                                     "--no-log-setup",
                                                     "--http", masterEndpoint(),
                                                     "--admin=" + masterAdminPort(),
                                                     "--domain", "",
                                                     "--zk", zk.connectString());
    if (!asList(args).contains("--name")) {
      argsList.add("--name");
      argsList.add(TEST_MASTER);
    }

    argsList.addAll(asList(args));

    return argsList;
  }

  protected MasterMain startDefaultMaster(String... args) throws Exception {
    final List<String> argsList = setupDefaultMaster(args);

    if (argsList == null) {
      return null;
    }

    final MasterMain master = startMaster(argsList.toArray(new String[argsList.size()]));
    waitForMasterToConnectToZK();

    return master;
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

  protected void startDefaultMasterDontWaitForZK(final CuratorClientFactory curatorClientFactory,
                                                 String... args) throws Exception {
    List<String> argsList = setupDefaultMaster(args);

    if (argsList == null) {
      return;
    }

    startMaster(curatorClientFactory, argsList.toArray(new String[argsList.size()]));
  }

  protected AgentMain startDefaultAgent(final String host, final String... args)
      throws Exception {
    if (isIntegration()) {
      checkArgument(args.length == 0,
                    "cannot start default agent in integration test with arguments passed");
      return null;
    }

    final String stateDir = agentStateDirs.resolve(host).toString();
    final List<String> argsList = Lists.newArrayList("-vvvv",
                                                     "--no-log-setup",
                                                     "--no-http",
                                                     "--name", host,
                                                     "--docker=" + DOCKER_HOST,
                                                     "--zk", zk.connectString(),
                                                     "--zk-session-timeout", "100",
                                                     "--zk-connection-timeout", "100",
                                                     "--state-dir", stateDir,
                                                     "--domain", "",
                                                     "--port-range=" +
                                                     dockerPortRange.lowerEndpoint() + ":" +
                                                     dockerPortRange.upperEndpoint()
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
    final String name = job.getId().getName();
    checkArgument(name.contains(testTag), "Job name must contain testTag to enable cleanup");

    final String serializedConfig = Json.asNormalizedString(job);
    final File configFile = temporaryFolder.newFile();
    Files.write(serializedConfig, configFile, Charsets.UTF_8);

    final List<String> args = ImmutableList.of("-q", "-f", configFile.getAbsolutePath());
    final String createOutput = cli("create", args);
    final String jobId = WHITESPACE.trimFrom(createOutput);

    return JobId.fromString(jobId);
  }

  protected void deployJob(final JobId jobId, final String host)
      throws Exception {
    final String deployOutput = cli("deploy", jobId.toString(), host);
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
    assertTrue(status == null ||
               status.getDeployments().get(host) == null);
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

  protected void awaitHostRegistered(final HeliosClient client, final String host,
                                     final int timeout,
                                     final TimeUnit timeUnit) throws Exception {
    Polling.await(timeout, timeUnit, new Callable<HostStatus>() {
      @Override
      public HostStatus call() throws Exception {
        return getOrNull(client.hostStatus(host));
      }
    });
  }

  protected HostStatus awaitHostStatus(final HeliosClient client, final String host,
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
      final DeploymentGroupStatus.State state)
      throws Exception {
    return Polling.await(LONG_WAIT_SECONDS, SECONDS, new Callable<DeploymentGroupStatus>() {
      @Override
      public DeploymentGroupStatus call() throws Exception {
        final DeploymentGroupStatusResponse response = getOrNull(
            client.deploymentGroupStatus(name));

        if (response != null) {
          final DeploymentGroupStatus status = response.getDeploymentGroupStatus();
          if (status.getState().equals(state)) {
            return status;
          } else if (status.getState().equals(DeploymentGroupStatus.State.FAILED)) {
            assertEquals(state, status.getState());
          }
        }

        return null;
      }
    });
  }

  protected <T> T getOrNull(final ListenableFuture<T> future)
      throws ExecutionException, InterruptedException {
    return Futures.withFallback(future, new FutureFallback<T>() {
      @Override
      public ListenableFuture<T> create(final Throwable t) throws Exception {
        return Futures.immediateFuture(null);
      }
    }).get();
  }

  protected String readLogFully(final ClientResponse logs) throws IOException {
    final LogReader logReader = new LogReader(logs.getEntityInputStream());
    StringBuilder stringBuilder = new StringBuilder();
    LogMessage logMessage;
    while ((logMessage = logReader.nextMessage()) != null) {
      stringBuilder.append(UTF_8.decode(logMessage.content()));
    }
    logReader.close();
    return stringBuilder.toString();
  }

  protected static void removeContainer(final DockerClient dockerClient, final String containerId)
      throws Exception {
    // Work around docker sometimes failing to remove a container directly after killing it
    Polling.await(1, MINUTES, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        try {
          dockerClient.killContainer(containerId);
          dockerClient.removeContainer(containerId);
          return true;
        } catch (ContainerNotFoundException e) {
          // We're done here
          return true;
        } catch (DockerException e) {
          if ((e instanceof DockerRequestException) &&
              ((DockerRequestException) e).message().contains(
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

  protected void assertJobEquals(final Job expected, final Job actual) {
    assertEquals(expected.toBuilder().setHash(actual.getId().getHash()).build(), actual);
  }
}

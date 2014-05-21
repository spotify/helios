package com.spotify.helios.agent.docker;

import com.google.common.base.Strings;
import com.google.common.net.HostAndPort;

import com.spotify.helios.agent.docker.messages.Container;
import com.spotify.helios.agent.docker.messages.ContainerConfig;
import com.spotify.helios.agent.docker.messages.ContainerCreation;
import com.spotify.helios.agent.docker.messages.ContainerInfo;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Optional.fromNullable;
import static java.lang.Long.toHexString;
import static java.lang.String.format;
import static java.lang.System.getenv;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;


public class DockerClientTest {

  public static final int DOCKER_PORT = Integer.valueOf(env("DOCKER_PORT", "4160"));
  public static final String DOCKER_HOST = env("DOCKER_HOST", ":" + DOCKER_PORT);
  public static final String DOCKER_ADDRESS;
  public static final String DOCKER_ENDPOINT;

  @Rule public ExpectedException exception = ExpectedException.none();

  static {
    // Parse DOCKER_HOST
    final String stripped = DOCKER_HOST.replaceAll(".*://", "");
    final HostAndPort hostAndPort = HostAndPort.fromString(stripped);
    final String host = hostAndPort.getHostText();
    DOCKER_ADDRESS = Strings.isNullOrEmpty(host) ? "localhost" : host;
    DOCKER_ENDPOINT = format("http://%s:%d", DOCKER_ADDRESS,
                             hostAndPort.getPortOrDefault(DOCKER_PORT));
  }

  private static String env(final String key, final String defaultValue) {
    return fromNullable(getenv(key)).or(defaultValue);
  }

  private final DockerClient sut = new DefaultDockerClient(URI.create(DOCKER_ENDPOINT));

  private final String nameTag = toHexString(ThreadLocalRandom.current().nextLong());

  @After
  public void removeContainers() throws Exception {
    final List<Container> containers = sut.listContainers();
    for (Container container : containers) {
      final ContainerInfo info = sut.inspectContainer(container.id());
      if (info != null && info.name().contains(nameTag)) {
        sut.killContainer(info.id());
        sut.removeContainer(info.id());
      }
    }
  }

  @Test
  public void integrationTest() throws Exception {

    // Create container
    final ContainerConfig config = new ContainerConfig();
    final String name = randomName();
    config.image("busybox");
    config.cmd(asList("sh", "-c", "while :; do sleep 1; done"));
    final ContainerCreation creation = sut.createContainer(config, name);
    final String id = creation.id();
    assertThat(creation.getWarnings(), anyOf(is(empty()), is(nullValue())));
    assertThat(id, is(any(String.class)));

    // Inspect using container ID
    {
      final ContainerInfo info = sut.inspectContainer(id);
      assertThat(info.config().image(), equalTo(config.image()));
      assertThat(info.config().cmd(), equalTo(config.cmd()));
    }

    // Inspect using container name
    {
      final ContainerInfo info = sut.inspectContainer(name);
      assertThat(info.config().image(), equalTo(config.image()));
      assertThat(info.config().cmd(), equalTo(config.cmd()));
    }

    // Start container
    sut.startContainer(id);

    // Kill container
    sut.killContainer(id);

    // Remove the container
    sut.removeContainer(id);

    // Verify that the container is gone
    exception.expect(ContainerNotFoundException.class);
    sut.inspectContainer(id);
  }

  private String randomName() {
    return nameTag + '-' + toHexString(ThreadLocalRandom.current().nextLong());
  }
}
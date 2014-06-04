package com.spotify.helios.agent.docker;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.CharStreams;

import com.spotify.helios.agent.docker.messages.Container;
import com.spotify.helios.agent.docker.messages.ContainerConfig;
import com.spotify.helios.agent.docker.messages.ContainerCreation;
import com.spotify.helios.agent.docker.messages.ContainerExit;
import com.spotify.helios.agent.docker.messages.ContainerInfo;
import com.spotify.helios.agent.docker.messages.HostConfig;
import com.spotify.helios.agent.docker.messages.ImageInfo;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.UniformInterface;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.ws.rs.core.MultivaluedMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.sun.jersey.api.client.config.ClientConfig.PROPERTY_READ_TIMEOUT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.HttpMethod.DELETE;
import static javax.ws.rs.HttpMethod.GET;
import static javax.ws.rs.HttpMethod.POST;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_OCTET_STREAM_TYPE;

public class DefaultDockerClient implements DockerClient {

  private static final long CONNECT_TIMEOUT_MILLIS = SECONDS.toMillis(5);
  private static final long READ_TIMEOUT_MILLIS = SECONDS.toMillis(30);

  private static final Logger log = LoggerFactory.getLogger(DefaultDockerClient.class);

  private static final String VERSION = "v1.11";
  private static final DefaultClientConfig CLIENT_CONFIG = new DefaultClientConfig(
      ObjectMapperProvider.class,
      LogsResponseReader.class,
      PullResponseReader.class);

  private static final Pattern CONTAINER_NAME_PATTERN = Pattern.compile("/?[a-zA-Z0-9_-]+");

  private final Client client;
  private final URI uri;

  public DefaultDockerClient(final String uri) {
    this(URI.create(uri));
  }

  public DefaultDockerClient(final URI uri) {
    this.uri = uri;
    this.client = Client.create(CLIENT_CONFIG);
    this.client.setConnectTimeout((int) CONNECT_TIMEOUT_MILLIS);
    this.client.setReadTimeout((int) READ_TIMEOUT_MILLIS);
  }

  @Override
  public List<Container> listContainers(final ListContainersParam... params)
      throws DockerException, InterruptedException {
    final Multimap<String, String> paramMap = ArrayListMultimap.create();
    for (ListContainersParam param : params) {
      paramMap.put(param.name(), param.value());
    }
    final WebResource resource = resource()
        .path("containers").path("json")
        .queryParams(multivaluedMap(paramMap));
    return request(GET, new GenericType<List<Container>>() {},
                   resource, resource.accept(APPLICATION_JSON_TYPE)
    );
  }

  @Override
  public ContainerCreation createContainer(final ContainerConfig config)
      throws DockerException, InterruptedException {
    return createContainer(config, null);
  }

  public static interface ExceptionPropagator {

    void propagate(UniformInterfaceException e) throws DockerException;
  }

  @Override
  public ContainerCreation createContainer(final ContainerConfig config,
                                           final String name)
  throws DockerException, InterruptedException {

    final MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    if (name != null) {
      checkArgument(CONTAINER_NAME_PATTERN.matcher(name).matches(),
                    "Invalid container name: \"%s\"", name);
      params.add("name", name);
    }

    try {
      final WebResource resource = resource()
          .path("containers").path("create")
          .queryParams(params);
      return request(POST, ContainerCreation.class, resource, resource
          .entity(config)
          .type(APPLICATION_JSON_TYPE)
          .accept(APPLICATION_JSON_TYPE));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ImageNotFoundException(config.image(), e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void startContainer(final String containerId)
      throws DockerException, InterruptedException {
    startContainer(containerId, HostConfig.builder().build());
  }

  @Override
  public void startContainer(final String containerId, final HostConfig hostConfig)
      throws DockerException, InterruptedException {
    checkNotNull(containerId, "containerId");
    checkNotNull(hostConfig, "hostConfig");
    try {
      final WebResource resource = resource()
          .path("containers").path(containerId).path("start");
      request(POST, resource, resource
          .type(APPLICATION_JSON_TYPE)
          .accept(APPLICATION_JSON_TYPE)
          .entity(hostConfig));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void killContainer(final String containerId) throws DockerException, InterruptedException {
    try {
      final WebResource resource = resource().path("containers").path(containerId).path("kill");
      request(POST, resource, resource);
    } catch (UniformInterfaceException e) {
      switch (e.getResponse().getStatus()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw new DockerException(e);
      }
    }
  }

  @Override
  public ContainerExit waitContainer(final String containerId)
      throws DockerException, InterruptedException {
    try {
      final WebResource resource = resource()
          .path("containers").path(containerId).path("wait");
      // Wait forever
      resource.setProperty(PROPERTY_READ_TIMEOUT, 0);
      return request(POST, ContainerExit.class, resource, resource.accept(APPLICATION_JSON_TYPE));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void removeContainer(final String containerId)
      throws DockerException, InterruptedException {
    removeContainer(containerId, false);
  }

  @Override
  public void removeContainer(final String containerId, final boolean removeVolumes)
      throws DockerException, InterruptedException {
    try {
      final WebResource resource = resource()
          .path("containers").path(containerId);
      request(DELETE, resource, resource
          .queryParam("v", String.valueOf(removeVolumes))
          .accept(APPLICATION_JSON_TYPE));
    } catch (UniformInterfaceException e) {
      switch (e.getResponse().getStatus()) {
        case 404:
          throw new ContainerNotFoundException(containerId);
        default:
          throw new DockerException(e);
      }
    }
  }

  @Override
  public ContainerInfo inspectContainer(final String containerId)
      throws DockerException, InterruptedException {
    try {
      final WebResource resource = resource().path("containers").path(containerId).path("json");
      return request(GET, ContainerInfo.class, resource, resource.accept(APPLICATION_JSON_TYPE));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public void pull(final String image) throws DockerException, InterruptedException {
    final ImageRef imageRef = new ImageRef(image);

    final MultivaluedMap<String, String> params = new MultivaluedMapImpl();
    params.add("fromImage", imageRef.getImage());
    if (imageRef.getTag() != null) {
      params.add("tag", imageRef.getTag());
    }

    final WebResource resource = resource().path("images").path("create").queryParams(params);

    try (ImagePull pull = request(POST, ImagePull.class, resource,
                                  resource.accept(APPLICATION_OCTET_STREAM_TYPE))) {
      pull.tail(image);
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ImageNotFoundException(image, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public ImageInfo inspectImage(final String image) throws DockerException, InterruptedException {
    try {
      final WebResource resource = resource().path("images").path(image).path("json");
      return request(GET, ImageInfo.class, resource, resource.accept(APPLICATION_JSON_TYPE));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ImageNotFoundException(image, e);
        default:
          throw e;
      }
    }
  }

  @Override
  public LogStream logs(final String containerId, final LogsParameter... params)
      throws DockerException, InterruptedException {
    final Multimap<String, String> paramMap = ArrayListMultimap.create();
    for (final LogsParameter param : params) {
      paramMap.put(param.name().toLowerCase(), String.valueOf(true));
    }
    final WebResource resource = resource()
        .path("containers").path(containerId).path("logs")
        .queryParams(multivaluedMap(paramMap));
    try {
      return request(GET, LogStream.class, resource,
                     resource.accept("application/vnd.docker.raw-stream"));
    } catch (DockerRequestException e) {
      switch (e.status()) {
        case 404:
          throw new ContainerNotFoundException(containerId);
        default:
          throw e;
      }
    }
  }

  private WebResource resource() {
    return client.resource(uri).path(VERSION);
  }

  private <T> T request(final String method, final GenericType<T> type,
                        final WebResource resource, final WebResource.Builder request)
      throws DockerException, InterruptedException {
    try {
      return request.method(method, type);
    } catch (ClientHandlerException e) {
      throw propagate(method, resource, e);
    } catch (UniformInterfaceException e) {
      throw propagate(method, resource, e);
    }
  }

  private <T> T request(final String method, final Class<T> clazz,
                        final WebResource resource, final UniformInterface request)
  throws DockerException, InterruptedException {
    try {
      return request.method(method, clazz);
    } catch (ClientHandlerException e) {
      throw propagate(method, resource, e);
    } catch (UniformInterfaceException e) {
      throw propagate(method, resource, e);
    }
  }

  private void request(final String method,
                       final WebResource resource,
                       final UniformInterface request) throws DockerException,
                                                              InterruptedException {
    try {
      request.method(method);
    } catch (ClientHandlerException e) {
      throw propagate(method, resource, e);
    } catch (UniformInterfaceException e) {
      throw propagate(method, resource, e);
    }
  }

  private DockerRequestException propagate(final String method, final WebResource resource,
                                           final UniformInterfaceException e) {
    return new DockerRequestException(method, resource.getURI(),
                                      e.getResponse().getStatus(), message(e.getResponse()),
                                      e);
  }

  private RuntimeException propagate(final String method, final WebResource resource,
                                     final ClientHandlerException e)
      throws DockerException, InterruptedException {
    final Throwable cause = e.getCause();
    if (cause instanceof SocketTimeoutException) {
      throw new DockerTimeoutException(method, resource.getURI(), e);
    } else if (cause instanceof InterruptedIOException) {
      throw new InterruptedException("Interrupted: " + method + " " + resource);
    } else {
      throw new DockerException(e);
    }
  }

  private String message(final ClientResponse response) {
    final Readable reader = new InputStreamReader(response.getEntityInputStream(), UTF_8);
    try {
      return CharStreams.toString(reader);
    } catch (IOException ignore) {
      return null;
    }
  }

  private MultivaluedMap<String, String> multivaluedMap(final Multimap<String, String> map) {
    final MultivaluedMap<String, String> multivaluedMap = new MultivaluedMapImpl();
    for (Map.Entry<String, String> e : map.entries()) {
      final String value = e.getValue();
      if (value != null) {
        multivaluedMap.add(e.getKey(), value);
      }
    }
    return multivaluedMap;
  }

}

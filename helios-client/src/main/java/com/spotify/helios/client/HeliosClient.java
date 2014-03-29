/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.client;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.spotify.helios.common.HeliosException;
import com.spotify.helios.common.Json;
import com.spotify.helios.common.Version;
import com.spotify.helios.common.VersionCompatibility;
import com.spotify.helios.common.VersionCompatibility.Status;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.HostStatus;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.HostDeregisterResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.descriptors.JobStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.SetGoalResponse;
import com.spotify.helios.common.protocol.TaskStatusEvents;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_SERVER_VERSION_HEADER;
import static com.spotify.helios.common.VersionCompatibility.HELIOS_VERSION_STATUS_HEADER;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HeliosClient {

  private static final Logger log = LoggerFactory.getLogger(HeliosClient.class);
  private static final long TIMEOUT_MILLIS = SECONDS.toMillis(30);

  private final String user;
  private final Supplier<List<URI>> endpointSupplier;

  private final ListeningExecutorService executorService;

  public HeliosClient(final String user,
                      final Supplier<List<URI>> endpointSupplier,
                      final ListeningExecutorService executorService) {
    this.user = user;
    this.endpointSupplier = endpointSupplier;
    this.executorService = executorService;
  }

  public HeliosClient(final String user, final List<URI> endpoints,
                      final ListeningExecutorService executorService) {
    this(user, Suppliers.ofInstance(endpoints), executorService);
  }

  public HeliosClient(final String user, final Supplier<List<URI>> endpointSupplier) {
    this(user, endpointSupplier, MoreExecutors.listeningDecorator(getExitingExecutorService(
        (ThreadPoolExecutor) newFixedThreadPool(4), 0, SECONDS)));
  }

  public HeliosClient(final String user, final List<URI> endpoints) {
    this(user, Suppliers.ofInstance(endpoints));
  }

  public void close() {
    executorService.shutdownNow();
  }

  private URI uri(final String path) {
    return uri(path, Collections.<String, String>emptyMap());
  }

  private URI uri(final String path, final Map<String, String> query) {
    // TODO: use a uri builder and clean this mess up
    checkArgument(path.startsWith("/"));
    final Map<String, String> queryWithUser = Maps.newHashMap(query);
    queryWithUser.put("user", user);
    final String queryPart = Joiner.on('&').withKeyValueSeparator("=").join(queryWithUser);
    try {
      return new URI("http", "helios", path, queryPart, null);
    } catch (URISyntaxException e) {
      throw Throwables.propagate(e);
    }
  }

  private String path(final String resource, final Object... params) {
    final String path;
    if (params.length == 0) {
      path = resource;
    } else {
      final List<String> encodedParams = Lists.newArrayList();
      for (final Object param : params) {
        final URI u;
        try {
          final String p = param.toString().replace("/", "%2F");
          // URI does path encoding right, but using it is painful
          u = new URI("http", "ignore", "/" + p, "");
        } catch (URISyntaxException e) {
          throw Throwables.propagate(e);
        }
        encodedParams.add(u.getRawPath().substring(1));
      }
      path = format(resource, encodedParams.toArray());
    }
    return path;
  }

  private ListenableFuture<Response> request(final URI uri, final String method) {
    return request(uri, method, null);
  }

  private ListenableFuture<Response> request(final URI uri, final String method,
                                             final Object entity) {

    final Map<String, List<String>> headers = Maps.newHashMap();
    final byte[] entityBytes;
    if (entity != null) {
      headers.put("Content-Type", asList("application/json"));
      headers.put("Charset", asList("utf-8"));
      headers.put("X-Helios-Version", asList(Version.POM_VERSION));
      entityBytes = Json.asBytesUnchecked(entity);
    } else {
      entityBytes = new byte[]{};
    }
    if (log.isTraceEnabled()) {
      log.trace("req: {} {} {} {} {} {}", method, uri,
                headers.size(),
                Joiner.on(',').withKeyValueSeparator("=").join(headers),
                entityBytes.length, entity == null ? "" : Json.asPrettyStringUnchecked(entity));
    } else {
      log.debug("req: {} {} {} {}", method, uri, headers.size(), entityBytes.length);
    }
    return executorService.submit(new Callable<Response>() {
      @Override
      public Response call() throws Exception {
        final HttpURLConnection connection = connect(uri, method, entityBytes, headers);
        final int status = connection.getResponseCode();
        final InputStream stream;
        if (status / 100 != 2) {
          stream = connection.getErrorStream();
        } else {
          stream = connection.getInputStream();
        }
        final ByteArrayOutputStream payload = new ByteArrayOutputStream();
        if (stream != null) {
          int n;
          byte[] buffer = new byte[4096];
          while ((n = stream.read(buffer, 0, buffer.length)) != -1) {
            payload.write(buffer, 0, n);
          }
        }
        if (log.isTraceEnabled()) {
          log.trace("rep: {} {} {} {} {}", method, uri, status, payload.size(), decode(payload));
        } else {
          log.debug("rep: {} {} {} {}", method, uri, status, payload.size());
        }
        checkprotocolVersionStatus(connection);
        return new Response(method, uri, status, payload.toByteArray());
      }
    });
  }

  private void checkprotocolVersionStatus(final HttpURLConnection connection) {
    final Status versionStatus = getVersionStatus(connection);
    if (versionStatus == null) {
      return; // shouldn't happen really
    }

    final String serverVersion = connection.getHeaderField(HELIOS_SERVER_VERSION_HEADER);
    if (versionStatus == VersionCompatibility.Status.MAYBE) {
      log.warn("Your Helios client version [{}] is ahead of the server [{}].  This will"
          + " probably work ok but there is the potential for weird things.  If in doubt,"
          + " contact the Helios team if you think the cluster you're connecting to is out of"
          + " date and should be upgraded.", Version.POM_VERSION, serverVersion);
    } else if (versionStatus == VersionCompatibility.Status.UPGRADE_SOON) {
      log.warn("Your Helios client is nearly out of date.  Please upgrade to [{}]",
          serverVersion);
    }
  }

  private Status getVersionStatus(final HttpURLConnection connection) {
    final String status = connection.getHeaderField(HELIOS_VERSION_STATUS_HEADER);
    if (status != null) {
      return VersionCompatibility.Status.valueOf(status);
    }
    return null;
  }

  private String decode(final ByteArrayOutputStream payload) {
    final byte[] bytes = payload.toByteArray();
    try {
      return Json.asPrettyString(Json.read(bytes, new TypeReference<Map<String, Object>>() {}));
    } catch (IOException e) {
      return new String(bytes, UTF_8);
    }
  }

  /**
   * Sets up a connection, retrying on connect failure.
   */
  private HttpURLConnection connect(final URI uri, final String method, final byte[] entity,
                                    final Map<String, List<String>> headers)
      throws URISyntaxException, IOException, TimeoutException, InterruptedException {
    final long deadline = currentTimeMillis() + TIMEOUT_MILLIS;
    final int offset = ThreadLocalRandom.current().nextInt();
    while (currentTimeMillis() < deadline) {
      final List<URI> endpoints = endpointSupplier.get();
      if (endpoints.isEmpty()) {
        throw new RuntimeException("failed to resolve master");
      }
      for (int i = 0; i < endpoints.size() && currentTimeMillis() < deadline; i++) {
        final URI endpoint = endpoints.get(positive(offset + i) % endpoints.size());
        final String fullpath = endpoint.getPath() + uri.getPath();
        final URI realUri = new URI("http", endpoint.getHost() + ":" + endpoint.getPort(),
                                    fullpath, uri.getQuery(), null);
        try {
          return connect0(realUri, method, entity, headers);
        } catch (ConnectException e) {
          // Connecting failed, sleep a bit to avoid hammering and then try another endpoint
          Thread.sleep(200);
        }
      }
      log.warn("Failed to connect, retrying in 5 seconds.");
      Thread.sleep(5000);
    }
    throw new TimeoutException("Timed out connecting to master");
  }

  private HttpURLConnection connect0(final URI uri, final String method, final byte[] entity,
                                     final Map<String, List<String>> headers)
      throws IOException {
    final HttpURLConnection connection;
    connection = (HttpURLConnection) uri.toURL().openConnection();
    connection.setInstanceFollowRedirects(false);
    if (entity != null && entity.length > 0) {
      for (Map.Entry<String, List<String>> header : headers.entrySet()) {
        for (final String value : header.getValue()) {
          connection.addRequestProperty(header.getKey(), value);
        }
      }
      connection.setDoOutput(true);
      connection.getOutputStream().write(entity);
    }
    setRequestMethod(connection, method);
    connection.getResponseCode();
    return connection;
  }

  private int positive(final int value) {
    return value < 0 ? value + Integer.MAX_VALUE : value;
  }

  private void setRequestMethod(final HttpURLConnection connection, final String method) {
    // Nasty workaround for ancient HttpURLConnection only supporting few methods
    final Class<?> httpURLConnectionClass = connection.getClass();
    try {
      final Field methodField = httpURLConnectionClass.getSuperclass().getDeclaredField("method");
      methodField.setAccessible(true);
      methodField.set(connection, method);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  private <T> ListenableFuture<T> get(final URI uri, final TypeReference<T> typeReference) {
    return get(uri, Json.type(typeReference));
  }

  private <T> ListenableFuture<T> get(final URI uri, final Class<T> clazz) {
    return get(uri, Json.type(clazz));
  }

  private <T> ListenableFuture<T> get(final URI uri, final JavaType javaType) {
    return transform(request(uri, "GET"), new ConvertResponseToPojo<T>(javaType));
  }

  private ListenableFuture<Integer> put(final URI uri) {
    return status(request(uri, "PUT"));
  }

  public ListenableFuture<JobDeployResponse> deploy(final Deployment job, final String host) {
    final Set<Integer> deserializeReturnCodes = ImmutableSet.of(HTTP_OK, HTTP_NOT_FOUND,
                                                                HTTP_BAD_METHOD,
                                                                HTTP_BAD_REQUEST);
    return transform(request(uri(path("/hosts/%s/jobs/%s", host, job.getJobId())), "PUT", job),
                     ConvertResponseToPojo.create(JobDeployResponse.class, deserializeReturnCodes));
  }

  public ListenableFuture<SetGoalResponse> setGoal(final Deployment job, final String host) {
    return transform(request(uri(path("/hosts/%s/jobs/%s", host, job.getJobId())), "PATCH", job),
                     ConvertResponseToPojo.create(SetGoalResponse.class,
                                                  ImmutableSet.of(HTTP_OK, HTTP_NOT_FOUND)));
  }

  private ListenableFuture<Integer> status(final ListenableFuture<Response> req) {
    return transform(req,
                     new Function<Response, Integer>() {
                       @Override
                       public Integer apply(final Response reply) {
                         return reply.status;
                       }
                     });
  }

  public ListenableFuture<Deployment> deployment(final String host, final JobId job) {
    return get(uri(path("/hosts/%s/jobs/%s", host, job)), Deployment.class);
  }

  public ListenableFuture<HostStatus> hostStatus(final String host) {
    return get(uri(path("/hosts/%s/status", host)), HostStatus.class);
  }

  public ListenableFuture<Integer> registerHost(final String host) {
    return put(uri(path("/hosts/%s", host)));
  }

  public ListenableFuture<JobDeleteResponse> deleteJob(final JobId id) {
    return transform(request(uri(path("/jobs/%s", id)), "DELETE"),
                     ConvertResponseToPojo.create(JobDeleteResponse.class,
                                                  ImmutableSet.of(HTTP_OK, HTTP_BAD_REQUEST)));
  }

  public ListenableFuture<JobUndeployResponse> undeploy(final JobId jobId, final String host) {
    return transform(request(uri(path("/hosts/%s/jobs/%s", host, jobId)), "DELETE"),
                     ConvertResponseToPojo.create(JobUndeployResponse.class,
                                                  ImmutableSet.of(HTTP_OK, HTTP_NOT_FOUND)));
  }

  public ListenableFuture<HostDeregisterResponse> deregisterHost(final String host) {
    return transform(request(uri(path("/hosts/%s", host)), "DELETE"),
                     ConvertResponseToPojo.create(HostDeregisterResponse.class,
                                                  ImmutableSet.of(HTTP_OK, HTTP_NOT_FOUND)));
  }

  public ListenableFuture<List<String>> listHosts() {
    return get(uri("/hosts/"), new TypeReference<List<String>>() {});
  }

  public ListenableFuture<List<String>> listMasters() {
    return get(uri("/masters/"), new TypeReference<List<String>>() {});
  }

  public ListenableFuture<CreateJobResponse> createJob(final Job descriptor) {
    return transform(request(uri("/jobs/"), "POST", descriptor),
                     ConvertResponseToPojo.create(CreateJobResponse.class,
                                                  ImmutableSet.of(HTTP_OK, HTTP_BAD_REQUEST)));
  }

  public ListenableFuture<Map<JobId, Job>> jobs(final String query) {
    return get(uri("/jobs", ImmutableMap.of("q", query)), new TypeReference<Map<JobId, Job>>() {});
  }

  public ListenableFuture<Map<JobId, Job>> jobs() {
    return get(uri("/jobs"), new TypeReference<Map<JobId, Job>>() {});
  }

  public ListenableFuture<TaskStatusEvents> jobHistory(final JobId jobId) {
    return transform(
        request(uri(path("/history/jobs/%s", jobId.toString())), "GET"),
        ConvertResponseToPojo.create(TaskStatusEvents.class,
                                     ImmutableSet.of(HTTP_OK, HTTP_NOT_FOUND)));
  }

  public ListenableFuture<JobStatus> jobStatus(final JobId jobId) {
    return get(uri(path("/jobs/%s/status", jobId)), JobStatus.class);
  }

  private static final class ConvertResponseToPojo<T> implements AsyncFunction<Response, T> {

    private final JavaType javaType;
    private final Set<Integer> decodeableStatusCodes;

    private ConvertResponseToPojo(JavaType javaType) {
      this(javaType, ImmutableSet.of(HTTP_OK));
    }

    public ConvertResponseToPojo(JavaType type, Set<Integer> decodeableStatusCodes) {
      this.javaType = type;
      this.decodeableStatusCodes = decodeableStatusCodes;
    }

    public static <T> ConvertResponseToPojo<T> create(Class<T> clazz,
                                                      Set<Integer> decodeableStatusCodes) {
      return new ConvertResponseToPojo<>(Json.type(clazz), decodeableStatusCodes);
    }

    @Override
    public ListenableFuture<T> apply(final Response reply)
        throws HeliosException {
      if (reply.status == HTTP_NOT_FOUND && !decodeableStatusCodes.contains(HTTP_NOT_FOUND)) {
        return immediateFuture(null);
      }

      if (!decodeableStatusCodes.contains(reply.status)) {
        throw new HeliosException("request failed: " + reply);
      }

      if (reply.payload.length == 0) {
        throw new HeliosException("bad reply: " + reply);
      }

      final T result;
      try {
        result = Json.read(reply.payload, javaType);
      } catch (IOException e) {
        throw new HeliosException("bad reply: " + reply, e);
      }

      return immediateFuture(result);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {

    private String user;
    private Supplier<List<URI>> endpointSupplier;

    public Builder setUser(final String user) {
      this.user = user;
      return this;
    }

    public Builder setEndpoints(final List<URI> endpoints) {
      this.endpointSupplier = Suppliers.ofInstance(endpoints);
      return this;
    }

    public HeliosClient build() {
      return new HeliosClient(user, endpointSupplier);
    }

    public Builder setEndpointSupplier(final Supplier<List<URI>> endpointSupplier) {
      this.endpointSupplier = endpointSupplier;
      return this;
    }
  }

  private class Response {

    private final String method;
    private final URI uri;
    private final int status;
    private final byte[] payload;

    public Response(final String method, final URI uri, final int status, final byte[] payload) {
      this.method = method;
      this.uri = uri;
      this.status = status;
      this.payload = payload;
    }

    @Override
    public String toString() {
      return "Response{" +
             "method='" + method + '\'' +
             ", uri=" + uri +
             ", status=" + status +
             ", payload=" + decode(payload) +
             '}';
    }

    private String decode(final byte[] payload) {
      if (payload == null) {
        return "";
      }
      final int length = Math.min(payload.length, 1024);
      return new String(payload, 0, length, UTF_8);
    }
  }
}

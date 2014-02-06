/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.common;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.spotify.helios.common.VersionCompatibility.Status;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Descriptor;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.AgentDeleteResponse;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeleteResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobStatus;
import com.spotify.helios.common.protocol.JobUndeployResponse;
import com.spotify.helios.common.protocol.SetGoalResponse;
import com.spotify.helios.common.protocol.TaskStatusEvents;
import com.spotify.hermes.Hermes;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.message.MessageBuilder;
import com.spotify.hermes.message.StatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.spotify.hermes.message.StatusCode.BAD_REQUEST;
import static com.spotify.hermes.message.StatusCode.FORBIDDEN;
import static com.spotify.hermes.message.StatusCode.METHOD_NOT_ALLOWED;
import static com.spotify.hermes.message.StatusCode.NOT_FOUND;
import static com.spotify.hermes.message.StatusCode.OK;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Client {

  private static final Logger log = LoggerFactory.getLogger(Client.class);

  private final String user;
  private final com.spotify.hermes.service.Client hermesClient;

  private boolean hasCheckedVersion;

  public Client(final String user, final com.spotify.hermes.service.Client hermesClient) {
    this.user = user;
    this.hermesClient = hermesClient;
  }

  public Client(final String user, final Iterable<String> endpoints) {
    this(user, Hermes.newClient(endpoints));
  }

  public void close() {
    hermesClient.close();
  }

  private URI uri(final String resource, final Object... args) {
    // TODO: use a uri builder and clean this mess up
    checkArgument(resource.startsWith("/"));
    final String path;
    if (args.length == 0) {
      path = resource;
    } else {
      Object[] encoded = ImmutableList.copyOf(transform(ImmutableList.copyOf(args),
          new Function<Object, String>() {
          @Override
          public String apply(Object arg0) {
            try {
              return java.net.URLEncoder.encode(arg0.toString(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
              throw new UnsupportedOperationException("SHOULD NOT HAPPEN, EVER");
            }
          }
        })).toArray(new String[args.length]);
      path = format(resource, encoded);
    }
    final String uri = "hm://helios" + path;
    final String q = uri.contains("?") ? "&" : "?";
    return URI.create(uri + q + "user=" + user);
  }

  private ListenableFuture<Message> request(final URI uri) {
    return request(Hermes.newRequestBuilder(uri.toString()));
  }

  private ListenableFuture<Message> request(final MessageBuilder messageBuilder) {
    doVersionCheck();
    final Message message = messageBuilder.setTtlMillis(TimeUnit.SECONDS.toMillis(30)).build();
    log.debug("request: {}", message);
    return hermesClient.send(message);
  }

  private void doVersionCheck() {
    if (hasCheckedVersion) {
      return;
    }

    try {
      hasCheckedVersion = true; // so as to avoid endless recursion while actually checking the version
      final VersionCheckResponse response = transform(
          request(uri("/version_check/%s", Version.POM_VERSION), "GET"),
          ConvertResponseToPojo.create(VersionCheckResponse.class, ImmutableSet.of(OK)))
          .get();
      Status status = response.getStatus();
      if (status == Status.INCOMPATIBLE) {
        throw new HeliosRuntimeException(format(
            "Server protocol version (%s) is incompatible with the client's (%s).  Upgrade your "
            + "Helios client to the recommended version (%s)",
            response.getServerVersion(), Version.POM_VERSION, response.getRecommendedVersion()));
      } else if (status == Status.MAYBE) {
        log.warn(format(
            "Server protocol version (%s) might not include features potentially used in your "
            + "Helios client (%s).  The current recommended client version is (%s)",
            response.getServerVersion(), Version.POM_VERSION, response.getRecommendedVersion()));
      } else if (status == Status.WARN) {
        log.warn(format("A newer Helios client version you want to use is available, please "
            + "upgrade to the recommended Helios client version (%s)",
            response.getRecommendedVersion()));
      }
    } catch (RuntimeException e) {
      hasCheckedVersion = false;
      throw e;
    } catch (InterruptedException | ExecutionException e) {
      hasCheckedVersion = false;
      throw new HeliosRuntimeException("Error checking client/server version compatibility", e);
    }
  }

  private ListenableFuture<Message> request(final URI uri, final String method) {
    return request(Hermes.newRequestBuilder(uri.toString(), method));
  }

  private ListenableFuture<Message> request(final URI uri, final String method,
                                            final Descriptor... descriptors) {
    doVersionCheck();
    final List<ByteString> payloadsJson = Lists.newArrayList();
    for (final Descriptor descriptor : descriptors) {
      payloadsJson.add(descriptor.toJsonByteString());
    }
    return request(Hermes.newRequestBuilder(uri.toString(), method)
                       .setPayloads(payloadsJson));
  }

  private <T> ListenableFuture<T> get(final URI uri, final TypeReference<T> typeReference) {
    return get(uri, Json.type(typeReference));
  }

  private <T> ListenableFuture<T> get(final URI uri, final Class<T> clazz) {
    return get(uri, Json.type(clazz));
  }

  private <T> ListenableFuture<T> get(final URI uri, final JavaType javaType) {
    return transform(
        request(uri),
        new ConvertResponseToPojo<T>(javaType));
  }

  private ListenableFuture<StatusCode> put(final URI uri) {
    return status(request(uri, "PUT"));
  }

  public ListenableFuture<JobDeployResponse> deploy(final Deployment job, final String host) {
    ImmutableSet<StatusCode> deserializeReturnCodes = ImmutableSet.of(OK, NOT_FOUND,
                                                                      METHOD_NOT_ALLOWED,
                                                                      BAD_REQUEST);
    return transform(request(uri("/agents/%s/jobs/%s", host, job.getJobId()), "PUT", job),
                     ConvertResponseToPojo.create(JobDeployResponse.class, deserializeReturnCodes));
  }

  public ListenableFuture<SetGoalResponse> setGoal(final Deployment job, final String host) {
    return transform(request(uri("/agents/%s/jobs/%s", host, job.getJobId()), "PATCH", job),
                     ConvertResponseToPojo.create(SetGoalResponse.class,
                                                  ImmutableSet.of(OK, NOT_FOUND)));
  }

  private ListenableFuture<StatusCode> status(final ListenableFuture<Message> req) {
    return transform(req,
                     new Function<Message, StatusCode>() {
                       @Override
                       public StatusCode apply(final Message reply) {
                         return reply.getStatusCode();
                       }
                     });
  }

  public ListenableFuture<Deployment> stat(final String agent, final JobId job) {
    return get(uri("/agents/%s/jobs/%s", agent, job), Deployment.class);
  }

  public ListenableFuture<AgentStatus> agentStatus(final String agent) {
    return get(uri("/agents/%s/status", agent), AgentStatus.class);
  }


  public ListenableFuture<StatusCode> registerAgent(final String agent) {
    return put(uri("/agents/%s", agent));
  }

  public ListenableFuture<JobDeleteResponse> deleteJob(final JobId id) {
    return transform(request(uri("/jobs/%s", id), "DELETE"),
                     ConvertResponseToPojo.create(JobDeleteResponse.class,
                                                  ImmutableSet.of(OK, FORBIDDEN)));
  }

  public ListenableFuture<JobUndeployResponse> undeploy(final JobId jobId, final String host) {
    return transform(request(uri("/agents/%s/jobs/%s", host, jobId), "DELETE"),
                     ConvertResponseToPojo.create(JobUndeployResponse.class,
                                                  ImmutableSet.of(OK, NOT_FOUND)));
  }

  public ListenableFuture<AgentDeleteResponse> deleteAgent(final String host) {
    return transform(request(uri("/agents/%s", host), "DELETE"),
                     ConvertResponseToPojo.create(AgentDeleteResponse.class,
                                                  ImmutableSet.of(OK, NOT_FOUND)));
  }

  public ListenableFuture<List<String>> listAgents() {
    return get(uri("/agents/"), new TypeReference<List<String>>() {});
  }

  public ListenableFuture<List<String>> listMasters() {
    return get(uri("/masters/"), new TypeReference<List<String>>() {});
  }

  public ListenableFuture<CreateJobResponse> createJob(final Job descriptor) {
    return transform(request(uri("/jobs/" + descriptor.getId()), "PUT", descriptor),
                     ConvertResponseToPojo.create(CreateJobResponse.class,
                                                  ImmutableSet.of(OK, BAD_REQUEST)));
  }

  public ListenableFuture<Map<JobId, Job>> jobs(final String query) {
    return get(uri("/jobs?q=%s", query), new TypeReference<Map<JobId, Job>>() {});
  }

  public ListenableFuture<Map<JobId, Job>> jobs() {
    return get(uri("/jobs"), new TypeReference<Map<JobId, Job>>() {});
  }

  public ListenableFuture<TaskStatusEvents> jobHistory(final JobId jobId) {
    return transform(
        request(uri("/history/jobs/%s", jobId.toString()), "GET"),
        ConvertResponseToPojo.create(TaskStatusEvents.class,
            ImmutableSet.of(OK, NOT_FOUND)));
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public ListenableFuture<JobStatus> jobStatus(final JobId jobId) {
    return get(uri("/jobs/%s/status", jobId), JobStatus.class);
  }

  private static final class ConvertResponseToPojo<T> implements AsyncFunction<Message, T> {

    private final JavaType javaType;
    private final ImmutableSet<StatusCode> decodeableStatusCodes;

    private ConvertResponseToPojo(JavaType javaType) {
      this(javaType, ImmutableSet.of(StatusCode.OK));
    }

    public ConvertResponseToPojo(JavaType type, ImmutableSet<StatusCode> decodeableStatusCodes) {
      this.javaType = type;
      this.decodeableStatusCodes = decodeableStatusCodes;
    }

    public static <T> ConvertResponseToPojo<T> create(Class<T> clazz,
                                                      ImmutableSet<StatusCode> immutableSet) {
      return new ConvertResponseToPojo<>(Json.type(clazz), immutableSet);
    }

    @Override
    public ListenableFuture<T> apply(final Message reply)
        throws HeliosException {
      StatusCode statusCode = reply.getStatusCode();
      if (statusCode == NOT_FOUND
          && !decodeableStatusCodes.contains(NOT_FOUND)) {
        return immediateFuture(null);
      }

      if (!decodeableStatusCodes.contains(statusCode)) {
        throw new HeliosException("request failed: " + reply);
      }

      if (reply.getPayloads().size() != 1) {
        throw new HeliosException("bad reply: " + reply);
      }

      final T result;
      final ByteString payload = reply.getPayloads().get(0);
      try {
        result = Json.read(payload.toByteArray(), javaType);
      } catch (IOException e) {
        throw new HeliosException("bad reply: " + reply, e);
      }

      return immediateFuture(result);
    }
  }

  public static class Builder {

    private com.spotify.hermes.service.Client client;
    private String user;
    private Iterable<String> endpoints;

    public Builder setUser(final String user) {
      this.user = user;
      return this;
    }

    public Builder setEndpoints(final Iterable<String> endpoints) {
      this.endpoints = endpoints;
      return this;
    }

    public Builder setEndpoints(final String... endpoints) {
      return setEndpoints(asList(endpoints));
    }

    public Builder setClient(final com.spotify.hermes.service.Client client) {
      this.client = client;
      return this;
    }

    public Client build() {
      if (client == null) {
        return new Client(user, endpoints);
      } else {
        return new Client(user, client);
      }
    }
  }
}

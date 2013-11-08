/**
 * Copyright (C) 2012 Spotify AB
 */

package com.spotify.helios.common;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.spotify.helios.common.descriptors.AgentJob;
import com.spotify.helios.common.descriptors.AgentStatus;
import com.spotify.helios.common.descriptors.Descriptor;
import com.spotify.helios.common.descriptors.JobDescriptor;
import com.spotify.hermes.Hermes;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.message.MessageBuilder;
import com.spotify.hermes.message.StatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.spotify.hermes.message.StatusCode.NOT_FOUND;
import static com.spotify.hermes.message.StatusCode.OK;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class Client {

  private static final Logger log = LoggerFactory.getLogger(Client.class);

  private final String user;
  private final com.spotify.hermes.service.Client hermesClient;

  public static final TypeReference<Map<String, JobDescriptor>> JOB_DESCRIPTOR_MAP =
      new TypeReference<Map<String, JobDescriptor>>() {};

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
    // TODO: use new uri builder in hermes and/or fix encoding
    checkArgument(resource.startsWith("/"));
    return URI.create("hm://helios" + format(resource, args) + "?user=" + user);
  }

  private ListenableFuture<Message> request(final URI uri) {
    return request(Hermes.newRequestBuilder(uri.toString()));
  }

  private ListenableFuture<Message> request(final MessageBuilder messageBuilder) {
    final Message message = messageBuilder.setTtlMillis(TimeUnit.SECONDS.toMillis(30)).build();
    log.debug("request: {}", message);
    return hermesClient.send(message);
  }

  private ListenableFuture<Message> request(final URI uri, final String method) {
    return request(Hermes.newRequestBuilder(uri.toString(), method));
  }

  private ListenableFuture<Message> request(final URI uri, final String method,
                                            final Descriptor... descriptors) {
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
        new AsyncFunction<Message, T>() {
          @Override
          public ListenableFuture<T> apply(
              final Message reply)
              throws HeliosException {
            if (reply.getStatusCode() == NOT_FOUND) {
              return immediateFuture(null);
            }

            if (reply.getStatusCode() != OK) {
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
        });
  }

  private ListenableFuture<StatusCode> patch(final URI uri, final Descriptor descriptor) {
    return status(request(uri, "PATCH", descriptor));
  }

  private ListenableFuture<StatusCode> put(final URI uri, final Descriptor descriptor) {
    return status(request(uri, "PUT", descriptor));
  }

  private ListenableFuture<StatusCode> put(final URI uri) {
    return status(request(uri, "PUT"));
  }

  public ListenableFuture<StatusCode> deploy(final AgentJob job, final String host) {
    return put(uri("/agents/%s/jobs/%s", host, job.getJob()), job);
  }

  public ListenableFuture<StatusCode> setGoal(final AgentJob job, final String host) {
    return patch(uri("/agents/%s/jobs/%s", host, job.getJob()), job);
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

  public ListenableFuture<AgentJob> stat(final String agent, final String job) {
    return get(uri("/agents/%s/jobs/%s", agent, job), AgentJob.class);
  }

  public ListenableFuture<AgentStatus> agentStatus(final String agent) {
    return get(uri("/agents/%s/status", agent), AgentStatus.class);
  }


  public ListenableFuture<StatusCode> registerAgent(final String agent) {
    return put(uri("/agents/%s", agent));
  }

  public ListenableFuture<StatusCode> undeploy(final String jobId, final String host) {
    return delete(uri("/agents/%s/jobs/%s", host, jobId));
  }

  private ListenableFuture<StatusCode> delete(final URI uri) {
    return status(request(uri, "DELETE"));
  }

  public ListenableFuture<List<String>> listAgents() {
    return get(uri("/agents/"), new TypeReference<List<String>>() {});
  }

  public ListenableFuture<StatusCode> createJob(final JobDescriptor descriptor) {
    return put(uri("/jobs/" + descriptor.getId()), descriptor);
  }

  public ListenableFuture<Map<String, JobDescriptor>> jobs() {
    return transform(
        request(uri("/jobs/")),
        new AsyncFunction<Message, Map<String, JobDescriptor>>() {
          @Override
          public ListenableFuture<Map<String, JobDescriptor>> apply(final Message reply)
              throws HeliosException {
            if (reply.getStatusCode() != StatusCode.OK) {
              throw new HeliosException("request failed: " + reply);
            }

            if (reply.getPayloads().size() != 1) {
              throw new HeliosException("bad reply: " + reply);
            }

            final Map<String, JobDescriptor> jobs;
            try {
              final ByteString payload = reply.getPayloads().get(0);
              jobs = Json.read(payload.toByteArray(), JOB_DESCRIPTOR_MAP);
            } catch (IOException e) {
              throw new HeliosException("bad reply: " + reply, e);
            }

            return immediateFuture(jobs);
          }
        });
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
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

    public Client build() {
      return new Client(user, endpoints);
    }
  }
}

/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.cli.command;

import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.helios.common.FutureReplyHandler;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.service.Client;
import com.spotify.hermes.service.ReplyHandler;

import java.util.Queue;
import java.util.concurrent.Semaphore;

/**
 * A hermes client that attempts to send requests up to a specified batch size.
 */
public class BatchingHermesClient implements com.spotify.hermes.service.Client {

  private final Client client;
  private final Semaphore batch;
  private final Queue<Request> queue;

  public BatchingHermesClient(final com.spotify.hermes.service.Client client,
                              final int batchSize,
                              final int queueSize) {
    this.client = client;
    this.batch = new Semaphore(batchSize);
    this.queue = Queues.newArrayBlockingQueue(queueSize);
  }

  @Override
  public void send(final Message message, final ReplyHandler replyHandler) {
    final boolean enqueued = queue.offer(new Request(message, replyHandler));
    if (!enqueued) {
      throw new RuntimeException("queue full");
    }
    pump();
  }

  @Override
  public ListenableFuture<Message> send(final Message message) {
    final FutureReplyHandler future = new FutureReplyHandler();
    send(message, future);
    return future;
  }

  @Override
  public void close() {
    client.close();
  }

  private void pump() {
    final boolean acquired = batch.tryAcquire();
    if (acquired) {
      final Request request = this.queue.poll();
      if (request == null) {
        batch.release();
        return;
      }
      request.send();
    }
  }

  private class Request implements ReplyHandler {

    private final Message message;
    private final ReplyHandler replyHandler;

    public Request(final Message message, final ReplyHandler replyHandler) {
      this.message = message;
      this.replyHandler = replyHandler;
    }

    @Override
    public void handleReply(final Message message) {
      batch.release();
      pump();
      replyHandler.handleReply(message);
    }

    @Override
    public void handleFailure(final Throwable throwable) {
      batch.release();
      pump();
      replyHandler.handleFailure(throwable);
    }

    @Override
    public void messageSent() {
      replyHandler.messageSent();
    }

    public void send() {
      client.send(message, this);
    }
  }
}

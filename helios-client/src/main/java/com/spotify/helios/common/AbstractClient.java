/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import com.google.common.util.concurrent.ListenableFuture;

import com.spotify.hermes.message.Message;
import com.spotify.hermes.service.Client;
import com.spotify.hermes.service.ReplyHandler;

/**
 * A convenient abstract class for implementing the hermes {@link com.spotify.hermes.service.Client}
 * interface.
 */
public abstract class AbstractClient implements Client {

  @Override
  public abstract void send(final Message request, final ReplyHandler replyHandler);

  @Override
  public ListenableFuture<Message> send(final Message request) {
    final FutureReplyHandler replyHandler = new FutureReplyHandler();
    send(request, replyHandler);
    return replyHandler;
  }

  @Override
  public void close() {
    // do nothing
  }
}

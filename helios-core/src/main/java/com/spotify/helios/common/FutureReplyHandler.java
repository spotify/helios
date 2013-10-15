/**
 * Copyright (C) 2013 Spotify AB
 */

package com.spotify.helios.common;

import com.google.common.util.concurrent.AbstractFuture;

import com.spotify.hermes.message.Message;
import com.spotify.hermes.service.ReplyHandler;

/**
 * A reply handler that also acts as a {@link com.google.common.util.concurrent.ListenableFuture}.
 */
public class FutureReplyHandler extends AbstractFuture<Message> implements ReplyHandler {

  @Override
  public void handleReply(final Message reply) {
    set(reply);
  }

  @Override
  public void handleFailure(final Throwable t) {
    setException(t);
  }

  @Override
  public void messageSent() {
    // ignore
  }
}

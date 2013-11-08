/*
 * Copyright (c) 2013 Spotify AB
 */

package com.spotify.helios.master.http;

import com.spotify.hermes.HermesRuntimeException;
import com.spotify.hermes.message.Message;
import com.spotify.hermes.message.StatusCode;
import com.spotify.hermes.service.ReplyHandler;
import com.spotify.hermes.service.ServiceRequest;

@SuppressWarnings("deprecation")
public class HttpServiceRequest implements ServiceRequest {

  private final Message request;
  private final ReplyHandler replyHandler;

  public HttpServiceRequest(final Message request, final ReplyHandler replyHandler) {
    this.request = request;
    this.replyHandler = replyHandler;
  }

  @Override
  public Message getMessage() {
    return request;
  }

  @Override
  public boolean reply(final Message msg) throws HermesRuntimeException {
    replyHandler.handleReply(msg);
    return true;
  }

  @Override
  public boolean reply(final StatusCode error) throws HermesRuntimeException {
    replyHandler.handleReply(request.makeReplyBuilder(error).build());
    return true;
  }

  @Override
  public boolean reply(final Message msg, final boolean noThrow) {
    return reply(msg);
  }

  @Override
  public boolean reply(final StatusCode status, final boolean noThrow) {
    return reply(status);
  }

  @Override
  public boolean drop() {
    return true;
  }

  @Override
  public boolean isExpired() {
    return false;
  }

  @Override
  public boolean canReply() {
    return true;
  }
}

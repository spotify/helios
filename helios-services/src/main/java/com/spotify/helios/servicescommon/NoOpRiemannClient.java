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

package com.spotify.helios.servicescommon;

import com.aphyr.riemann.Proto.Msg;
import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.IPromise;
import com.aphyr.riemann.client.Promise;

import java.io.IOException;

/**
 * A Riemann client that does nothing.
 */
public class NoOpRiemannClient extends AbstractRiemannClient {
  private static final Msg OK_MESSAGE = Msg.newBuilder().setOk(true).build();
  private static final IPromise<Msg> OK_PROMISE;
  static {
    OK_PROMISE = new Promise<Msg>();
    OK_PROMISE.deliver(OK_MESSAGE);
  }

  @Override
  public IPromise<Msg> aSendMaybeRecvMessage(Msg arg0) {
    return OK_PROMISE;
  }

  @Override
  public IPromise<Msg> aSendRecvMessage(Msg arg0) {
    return OK_PROMISE;
  }

  @Override
  public void connect() throws IOException {
  }

  @Override
  public void disconnect() throws IOException {
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public boolean isConnected() {
    return true;
  }

  @Override
  public void reconnect() throws IOException {
  }

  @Override
  public Msg sendMaybeRecvMessage(Msg arg0) throws IOException {
    return OK_MESSAGE;
  }

  @Override
  public Msg sendRecvMessage(Msg arg0) throws IOException {
    return OK_MESSAGE;
  }

  public RiemannFacade facade() {
    return new RiemannFacade(this, "fakehost", "fakeservice");
  }
}


package com.spotify.helios.servicescommon;

import com.aphyr.riemann.Proto.Msg;
import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.IPromise;
import com.aphyr.riemann.client.Promise;

import java.io.IOException;

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


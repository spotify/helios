package com.spotify.helios.servicescommon;

import com.aphyr.riemann.Proto.Msg;
import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.IPromise;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NoOpRiemannClient extends AbstractRiemannClient {
  private static final Msg OK_MESSAGE = Msg.newBuilder().setOk(true).build();
  private static final IPromise<Msg> OK_PROMISE = new IPromise<Msg>() {
    @Override
    public void deliver(Object arg0) {
    }

    @Override
    public Msg deref() throws IOException {
      return OK_MESSAGE;
    }

    @Override
    public Msg deref(long arg0, TimeUnit arg1) throws IOException {
      return OK_MESSAGE;
    }

    @Override
    public Msg deref(long arg0, TimeUnit arg1, Msg arg2) throws IOException {
      return OK_MESSAGE;
    }
  };

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

  public static RiemannFacade facade() {
    return new RiemannFacade(new NoOpRiemannClient());
  }
}


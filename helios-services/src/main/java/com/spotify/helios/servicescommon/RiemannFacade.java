package com.spotify.helios.servicescommon;

import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.EventDSL;

public class RiemannFacade {
  private final AbstractRiemannClient client;

  public RiemannFacade(AbstractRiemannClient client) {
    this.client = client;
  }

  public EventDSL event() {
    return client.event();
  }
}

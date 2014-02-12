package com.spotify.helios.servicescommon;

import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.EventDSL;

public class RiemannFacade {
  private static final Integer EVENT_TIME_TO_LIVE = 600;

  private final AbstractRiemannClient client;
  private final String hostName;
  private final String service;

  public RiemannFacade(AbstractRiemannClient client, String hostName, String service) {
    this.client = client;
    this.hostName = hostName;
    this.service = service;
  }

  public EventDSL event() {
    return client.event()
        .time(System.currentTimeMillis() / 1000.0)
        .ttl(EVENT_TIME_TO_LIVE)
        .host(hostName)
        .service(service);
  }

  public RiemannFacade stack(String subService) {
    return new RiemannFacade(client, hostName, service + "/" + subService);
  }
}

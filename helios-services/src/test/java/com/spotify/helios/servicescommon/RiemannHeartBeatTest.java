package com.spotify.helios.servicescommon;

import com.aphyr.riemann.Proto.Event;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class RiemannHeartBeatTest {

  @Test
  public void test() throws Exception {
    CapturingRiemannClient client = new CapturingRiemannClient();

    final RiemannFacade facade = new RiemannFacade(client, "HOSTNAME", "SERVICE");

    RiemannHeartBeat hb = new RiemannHeartBeat(TimeUnit.MILLISECONDS, 1, facade);
    hb.start();
    Thread.sleep(250); // should get something in that time frame
    List<Event> events = client.getEvents();
    assertFalse(events.isEmpty());

    Event event = events.get(0);
    assertEquals("ok", event.getState());
  }
}

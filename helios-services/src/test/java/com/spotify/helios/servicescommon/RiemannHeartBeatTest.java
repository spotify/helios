package com.spotify.helios.servicescommon;

import com.aphyr.riemann.Proto.Event;
import com.spotify.helios.Polling;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RiemannHeartBeatTest {

  @Test
  public void test() throws Exception {
    final CapturingRiemannClient client = new CapturingRiemannClient();

    final RiemannFacade facade = new RiemannFacade(client, "HOSTNAME", "SERVICE");

    final RiemannHeartBeat hb = new RiemannHeartBeat(TimeUnit.MILLISECONDS, 1, facade);
    hb.start();

    final List<Event> events = Polling.await(10, TimeUnit.SECONDS, new Callable<List<Event>>() {
      @Override
      public List<Event> call() throws Exception {
        final List<Event> events = client.getEvents();
        return events != null && !events.isEmpty() ? events : null;
      }
    });

    final Event event = events.get(0);
    assertEquals("ok", event.getState());
  }
}

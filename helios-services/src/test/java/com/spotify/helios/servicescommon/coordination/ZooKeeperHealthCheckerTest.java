package com.spotify.helios.servicescommon.coordination;

import com.aphyr.riemann.Proto.Event;
import com.spotify.helios.ZooKeeperStandaloneServerManager;
import com.spotify.helios.servicescommon.CapturingRiemannClient;
import com.spotify.helios.servicescommon.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.RiemannFacade;
import com.yammer.metrics.core.HealthCheck.Result;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZooKeeperHealthCheckerTest {
  private CapturingRiemannClient riemannClient;

  @Before
  public void setUp() throws Exception {
    riemannClient = new CapturingRiemannClient();
  }

  @Test
  public void test() throws Exception {
    final RiemannFacade facade = new RiemannFacade(riemannClient, "HOSTNAME", "SERVICE");
    final ZooKeeperStandaloneServerManager zk = new ZooKeeperStandaloneServerManager();
    final DefaultZooKeeperClient client = new DefaultZooKeeperClient(zk.curator());

    ZooKeeperHealthChecker hc = new ZooKeeperHealthChecker(client, "/", facade, MILLISECONDS, 1);
    hc.start();

    // Start in our garden of eden where everything travaileth together in harmony....
    Thread.sleep(2000);
    Result result = hc.check();
    assertTrue(result.isHealthy());

    // Alas!  Behold!  Our zookeeper hath been slain with the sword of the wrath of the random!
    zk.stop();
    Thread.sleep(2000);
    checkForState("critical");

    // And lo, our zookeeper hath been resurrected and our foe vanquished!
    zk.start();
    Thread.sleep(2000);
    result = hc.check();
    assertTrue(result.isHealthy());
    checkForState("ok");

    // And they lived happily ever after
  }

  private void checkForState(String expectedState) {
    final List<Event> events = riemannClient.getEvents();
    assertFalse(events.isEmpty());
    final Event event = events.get(0);
    try {
      assertEquals(expectedState, event.getState());
    } finally {
      riemannClient.clearEvents();
    }
  }
}

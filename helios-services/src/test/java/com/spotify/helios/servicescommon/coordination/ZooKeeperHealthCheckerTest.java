package com.spotify.helios.servicescommon.coordination;

import com.aphyr.riemann.Proto.Event;
import com.spotify.helios.Polling;
import com.spotify.helios.ZooKeeperStandaloneServerManager;
import com.spotify.helios.servicescommon.CapturingRiemannClient;
import com.spotify.helios.servicescommon.DefaultZooKeeperClient;
import com.spotify.helios.servicescommon.RiemannFacade;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

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
    awaitHealthy(hc, 1, MINUTES);

    // Alas!  Behold!  Our zookeeper hath been slain with the sword of the wrath of the random!
    zk.stop();
    awaitState("critical", 1, MINUTES);

    // And lo, our zookeeper hath been resurrected and our foe vanquished!
    zk.start();
    awaitState("ok", 1, MINUTES);
    awaitHealthy(hc, 1, MINUTES);

    // And they lived happily ever after
  }

  private void awaitHealthy(final ZooKeeperHealthChecker hc, final int duration,
                            final TimeUnit timeUnit) throws Exception {
    Polling.await(duration, timeUnit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        return hc.check().isHealthy() ? true : null;
      }
    });
  }

  private void awaitState(final String state, final int duration, final TimeUnit timeUnit)
      throws Exception {
    Polling.await(duration, timeUnit, new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        final List<Event> events = riemannClient.getEvents();
        if (events.isEmpty()) {
          return null;
        }
        final Event event = events.get(0);
        return state.equals(event.getState()) ? event : null;
      }
    });
  }
}

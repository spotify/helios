package com.spotify.helios.servicescommon;

import com.yammer.dropwizard.lifecycle.Managed;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.getExitingScheduledExecutorService;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RiemannHeartBeat implements Managed {
  private final ScheduledExecutorService scheduler;
  private final int interval;
  private final TimeUnit timeUnit;
  private final RiemannFacade facade;

  public RiemannHeartBeat(final TimeUnit timeUnit, final int interval,
                          final RiemannFacade riemannFacade) {
    this.scheduler = getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1),
                                                        0, SECONDS);
    this.timeUnit = timeUnit;
    this.interval = interval;
    this.facade = riemannFacade.stack("heartbeat");
  }

  @Override
  public void start() throws Exception {
    scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        facade.event()
            .state("ok")
            .metric(1.0)
            .tags("heartbeat")
            .ttl(timeUnit.toSeconds(interval * 3))
            .send();

      }
    }, 0, interval, timeUnit);
  }

  @Override
  public void stop() throws Exception {
    scheduler.shutdownNow();
  }

}

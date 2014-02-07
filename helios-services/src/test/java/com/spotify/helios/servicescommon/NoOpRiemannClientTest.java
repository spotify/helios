package com.spotify.helios.servicescommon;

import org.junit.Test;

public class NoOpRiemannClientTest {

  @Test
  public void test() {
    // quick test to make sure nothing goes kaboom since we assume some behavior of the
    // abstract client
    NoOpRiemannClient c = new NoOpRiemannClient();
    c.event().
        service("fridge").
        state("running").
        metric(5.3).
        tags("appliance", "cold").
        send();
  }

}

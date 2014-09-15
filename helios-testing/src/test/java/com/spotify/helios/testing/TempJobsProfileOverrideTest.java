package com.spotify.helios.testing;

import com.typesafe.config.Config;

import org.junit.Test;

import java.util.Properties;

import static com.spotify.helios.testing.TemporaryJobs.HELIOS_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;

public class TempJobsProfileOverrideTest {
  @Test public void foo() throws Exception {
    final Properties oldProperties = System.getProperties();
    final Config c;
    try {
      System.setProperty(HELIOS_TESTING_PROFILE, "this");
      c = TemporaryJobs.loadConfig();
    } finally {
      System.setProperties(oldProperties);
    }
    assertEquals("this", TemporaryJobs.getProfileFromConfig(c));
  }
}

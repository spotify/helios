package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ExternalPortTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testEquals() throws Exception {
    assertEquals(ExternalPort.of(52342), ExternalPort.of(52342));
    assertEquals(ExternalPort.of(null), null);
    assertThat(ExternalPort.of(28437), not(equalTo(ExternalPort.of(12312))));
  }

  @Test
  public void testSerialize() throws Exception {
    final ExternalPort x = ExternalPort.of(3);
    assertEquals("3", OBJECT_MAPPER.writeValueAsString(x));
    assertEquals("null", OBJECT_MAPPER.writeValueAsString(ExternalPort.of((Integer) null)));
  }

  @Test
  public void testDeserialize() throws Exception {
    assertEquals((Integer) 3, OBJECT_MAPPER.readValue("3", ExternalPort.class).get());
  }

  @Test
  public void testNull() throws Exception {
    // Unfortunately, I cannot get Jackson to deserialize this as ExternalPort.of(null)
    // with EP wrapping the null instead of returning it.
    assertEquals(null, OBJECT_MAPPER.readValue("null", ExternalPort.class));
  }
}

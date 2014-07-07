package com.spotify.helios.common.descriptors;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AppProtocolTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testSerialize() throws Exception {
    assertEquals("\"hermes\"", OBJECT_MAPPER.writeValueAsString(AppProtocol.of("hermes")));
    assertEquals("null", OBJECT_MAPPER.writeValueAsString(AppProtocol.of(null)));
  }

  @Test
  public void testDeserialize() throws Exception {
    assertEquals("hermes", OBJECT_MAPPER.readValue("\"hermes\"", AppProtocol.class).get());
  }

  @Test
  public void testNull() throws Exception {
    assertNull(null, OBJECT_MAPPER.readValue("null", AppProtocol.class));
  }

  @Test
  public void testEquals() throws Exception {
    // work to disable any constant folding by the compiler
    String foo = "foo";
    foo = foo + "X";
    foo = foo.substring(0, 3);

    assertEquals(AppProtocol.of("foobar"), AppProtocol.of(foo + "bar"));
  }
}

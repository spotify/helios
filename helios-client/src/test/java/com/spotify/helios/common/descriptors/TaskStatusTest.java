package com.spotify.helios.common.descriptors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import com.spotify.helios.common.Json;
import com.spotify.helios.common.descriptors.TaskStatus.State;

import org.junit.Test;

import java.util.Map;

import static com.spotify.helios.common.descriptors.Goal.START;
import static org.junit.Assert.assertEquals;

public class TaskStatusTest {
  private static final Job JOB = Job.newBuilder()
      .setCommand(ImmutableList.of("BOGUS"))
      .setImage("IMAGE")
      .setName("NAME")
      .setVersion("VERSION")
      .build();
  private static final Map<String, String> ENV = ImmutableMap.<String, String>builder()
      .put("VAR", "VALUE")
      .build();
  private static final TaskStatus STATUS = TaskStatus.newBuilder()
      .setContainerId("CONTAINER_ID")
      .setGoal(START)
      .setJob(JOB)
      .setState(State.RUNNING)
      .setEnv(ENV)
      .setThrottled(ThrottleState.NO)
      .build();

  @Test
  public void testSerializationOfEnvironment() throws Exception {

    byte[] bytes = Json.asBytes(STATUS);

    final TaskStatus read = Json.read(bytes, TaskStatus.class);
    assertEquals(1, read.getEnv().size());
    assertEquals("VALUE", read.getEnv().get("VAR"));
  }

  @Test
  public void testToBuilderAndBackEnvironment() throws Exception {
    TaskStatus s = STATUS.asBuilder().build();
    assertEquals(1, s.getEnv().size());
    assertEquals("VALUE", s.getEnv().get("VAR"));
  }
}

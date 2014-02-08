/**
 * Copyright (C) 2014 Spotify AB
 */

package com.spotify.helios.system;

public class JobHistoryTest {
  // TODO (dano): restore task status history and make sure that it's bounded as well
//  @Test
//  public void testJobHistory() throws Exception {
//    startDefaultMaster();
//
//    final Client client = Client.newBuilder()
//        .setUser(TEST_USER)
//        .setEndpoints(masterEndpoint)
//        .build();
//
//    startDefaultAgent(TEST_AGENT);
//    JobId jobId = createJob(JOB_NAME, JOB_VERSION, "busybox", ImmutableList.of("/bin/true"));
//    deployJob(jobId, TEST_AGENT);
//    awaitJobState(client, TEST_AGENT, jobId, EXITED, LONG_WAIT_MINUTES, MINUTES);
//    undeployJob(jobId, TEST_AGENT);
//    TaskStatusEvents events = client.jobHistory(jobId).get();
//    List<TaskStatusEvent> eventsList = events.getEvents();
//    assertFalse(eventsList.isEmpty());
//
//    final TaskStatusEvent event1 = eventsList.get(0);
//    assertEquals(State.CREATING, event1.getStatus().getState());
//    assertNull(event1.getStatus().getContainerId());
//
//    final TaskStatusEvent event2 = eventsList.get(1);
//    assertEquals(State.STARTING, event2.getStatus().getState());
//    assertNotNull(event2.getStatus().getContainerId());
//
//    final TaskStatusEvent event3 = eventsList.get(2);
//    assertEquals(State.RUNNING, event3.getStatus().getState());
//
//    final TaskStatusEvent event4 = eventsList.get(3);
//    assertEquals(State.EXITED, event4.getStatus().getState());
//  }
}

package com.spotify.helios.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.PortMapping;

import org.junit.Test;

import static com.spotify.helios.common.descriptors.Job.EMPTY_COMMAND;
import static com.spotify.helios.common.descriptors.Job.EMPTY_ENV;
import static com.spotify.helios.common.descriptors.Job.EMPTY_PORTS;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class JobValidatorTest {

  final JobValidator validator = new JobValidator();

  @Test
  public void testValidJobPasses() {
    final Job job = Job.newBuilder()
        .setName("foo")
        .setVersion("1")
        .setImage("bar")
        .setEnv(ImmutableMap.of("FOO", "BAR"))
        .setPorts(ImmutableMap.of("1", PortMapping.of(1, 1),
                                  "2", PortMapping.of(2, 2)))
        .build();
    assertThat(validator.validate(job), is(empty()));
  }

  @Test
  public void testPortMappingCollissionFails() throws Exception {
    final Job job = Job.newBuilder()
        .setName("foo")
        .setVersion("1")
        .setImage("bar")
        .setPorts(ImmutableMap.of("1", PortMapping.of(1, 1),
                                  "2", PortMapping.of(2, 1)))
        .build();

    assertEquals(ImmutableSet.of("Duplicate external port mapping: 1"), validator.validate(job));
  }

  @Test
  public void testIdMismatchFails() throws Exception {
    final Job job = new Job(JobId.fromString("foo:bar:badf00d"),
                            "bar", EMPTY_COMMAND, EMPTY_ENV, EMPTY_PORTS, "foobar");
    final JobId recomputedId = job.toBuilder().build().getId();
    assertEquals(ImmutableSet.of("Id mismatch: " + job.getId() + " != " + recomputedId),
                 validator.validate(job));
  }
}

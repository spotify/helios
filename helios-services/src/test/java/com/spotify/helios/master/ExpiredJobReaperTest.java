package com.spotify.helios.master;

import com.google.common.collect.ImmutableMap;

import com.spotify.helios.agent.Clock;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.JobStatus;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.Date;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ExpiredJobReaperTest {

  @Mock private MasterModel masterModel;
  @Mock private Clock mockClock;

  private static final JobId NON_EXPIRING_JOB_ID = JobId.fromString("non_expiring");
  private static final Job NON_EXPIRING_JOB = Job.newBuilder()
      .setCommand(asList("foo", "foo"))
      .setImage("foo:4711")
      .setName("foo")
      .setVersion("17")
      .build();

  private static final JobId EXPIRING_JOB_ID = JobId.fromString("expiring");

  private static final long EXPIRED_TS = 0;
  private static final long CURRENT_TS = 1;
  private static final long FUTURE_TS = 2;

  private static final Job EXPIRING_JOB = Job.newBuilder()
      .setCommand(asList("foo", "foo"))
      .setImage("foo:4711")
      .setName("foo")
      .setVersion("17")
      .setExpires(new Date(EXPIRED_TS))
      .build();

  private static final JobId FAR_FUTURE_EXPIRING_JOB_ID = JobId.fromString("far_future_expiring");
  private static final Job FAR_FUTURE_EXPIRING_JOB = Job.newBuilder()
      .setCommand(asList("foo", "foo"))
      .setImage("foo:4711")
      .setName("foo")
      .setVersion("17")
      .setExpires(new Date(FUTURE_TS))
      .build();

  private static final Map<JobId, Job> JOBS = ImmutableMap.of(
      NON_EXPIRING_JOB_ID, NON_EXPIRING_JOB,
      EXPIRING_JOB_ID, EXPIRING_JOB,
      FAR_FUTURE_EXPIRING_JOB_ID, FAR_FUTURE_EXPIRING_JOB
  );

  @Test
  public void testExpiredJobReaper() throws Exception {
    when(mockClock.now()).thenReturn(new Instant(CURRENT_TS));
    when(masterModel.getJobs()).thenReturn(JOBS);

    when(masterModel.getJobStatus(any(JobId.class)))
      .then(new Answer<JobStatus>() {
        @Override
        public JobStatus answer(final InvocationOnMock invocation) throws Throwable {
          JobId jobId = (JobId) invocation.getArguments()[0];

          Map<String, Deployment> deployments = ImmutableMap.of(
              "hostA", Deployment.of(jobId, Goal.START),
              "hostB", Deployment.of(jobId, Goal.START));

          return JobStatus.newBuilder()
              .setJob(JOBS.get(jobId))
              .setDeployments(deployments)
              .build();
        }
      });

    ExpiredJobReaper.newBuilder()
        .setClock(mockClock)
        .setMasterModel(masterModel)
        .build()
        .runOneIteration();

    // Make sure that the expiring job was removed, but that the non-expiring job
    // and the job that expires far in the future were not.
    verify(masterModel).undeployJob(eq("hostA"), eq(EXPIRING_JOB_ID));
    verify(masterModel).undeployJob(eq("hostB"), eq(EXPIRING_JOB_ID));
    verify(masterModel).removeJob(eq(EXPIRING_JOB_ID));

    verifyNoMoreInteractions(ignoreStubs(masterModel));
  }
}
package com.spotify.helios;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import com.spotify.helios.common.HeliosClient;
import com.spotify.helios.common.descriptors.Deployment;
import com.spotify.helios.common.descriptors.Goal;
import com.spotify.helios.common.descriptors.Job;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.TaskStatus;
import com.spotify.helios.common.protocol.CreateJobResponse;
import com.spotify.helios.common.protocol.JobDeployResponse;
import com.spotify.helios.common.protocol.JobStatus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.spotify.helios.Polling.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicFunctionalityITCase {
  private static final Logger log = LoggerFactory.getLogger(BasicFunctionalityITCase.class);
  private static final String JOB_NAME = "BasicFunctionalityITCase";

  static final List<String> DO_NOTHING_COMMAND = ImmutableList.of(
      "sh", "-c", "while :; do sleep 1; done");

  private HeliosClient client = null;
  private String deployHost = null;
  private JobId id = null;

  @Before
  public void setUp() throws Exception {
    String endpointUrl = System.getenv("HELIOS_ENDPOINT");
    if (endpointUrl == null) {
      endpointUrl = "http://localhost:5801";
    }
    client = new HeliosClient("unittest", ImmutableList.of(new URI(endpointUrl)));
  }

  @After
  public void tearDown() throws Exception {
    if (client == null) {
      log.error("Client is null");
      return;
    }
    if (deployHost != null) {
      log.error("undeploy");
      client.undeploy(id, deployHost).get();
    }
    if (id != null) {
      log.error("delete job");
      client.deleteJob(id).get();
    }
  }

  @Test
  public void deployJobx() throws Exception {
    final CreateJobResponse createResult = client.createJob(Job.newBuilder()
        .setVersion("" + System.currentTimeMillis())
        .setName(JOB_NAME)
        .setImage("ubuntu:12.04")
        .setCommand(DO_NOTHING_COMMAND)
        .build()).get();
    id = JobId.fromString(createResult.getId());

    assertEquals(CreateJobResponse.Status.OK, createResult.getStatus());

    final Map<JobId, Job> jobMap = client.jobs(JOB_NAME).get();
    boolean found = false;
    for (Entry<JobId, Job> entry : jobMap.entrySet()) {
      if (entry.getKey().toString().equals(createResult.getId())) {
        found = true;
        break;
      }
    }
    assertTrue("expected to find job I just created in results", found);
  }

  @Test
  public void deployJob() throws Exception {
    final CreateJobResponse createResult = client.createJob(Job.newBuilder()
      .setVersion("" + System.currentTimeMillis())
      .setName(JOB_NAME)
      .setImage("ubuntu:12.04")
      .setCommand(DO_NOTHING_COMMAND)
      .build()).get();
    id = JobId.fromString(createResult.getId());

    assertEquals(CreateJobResponse.Status.OK, createResult.getStatus());

    final List<String> hosts = client.listHosts().get();
    deployHost = Iterables.get(hosts, 0);
    final JobDeployResponse deployResult = client.deploy(new Deployment(id, Goal.START),
        deployHost).get();
    assertEquals(JobDeployResponse.Status.OK, deployResult.getStatus());

    // Wait for it to be running
    final Boolean ok = await(30, TimeUnit.SECONDS, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        final JobStatus result = client.jobStatus(id).get();
        final Set<Entry<String, TaskStatus>> statuses = result.getTaskStatuses().entrySet();
        if (statuses.isEmpty()) {
          return null;
        }

        final Entry<String, TaskStatus> last = Iterables.getLast(statuses);
        final TaskStatus status = last.getValue();

        if (!last.getKey().equals(deployHost)) {
          return false;  // something went awry
        }

        if (TaskStatus.State.RUNNING == status.getState()) {
          return true;
        }
        return null;
      }
    });
    assertTrue("deployed to wrong host?!?!", ok);
  }
}

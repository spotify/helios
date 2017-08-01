/*-
 * -\-\-
 * Helios Services
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.helios.master.resources;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.HostSelector;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.descriptors.RolloutOptions;
import com.spotify.helios.common.protocol.CreateDeploymentGroupResponse;
import com.spotify.helios.common.protocol.RemoveDeploymentGroupResponse;
import com.spotify.helios.common.protocol.RollingUpdateRequest;
import com.spotify.helios.common.protocol.RollingUpdateResponse;
import com.spotify.helios.master.DeploymentGroupDoesNotExistException;
import com.spotify.helios.master.DeploymentGroupExistsException;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.MasterModel;
import javax.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DeploymentGroupResourceTest {

  private static final HostSelector ROLE_SELECTOR = HostSelector.parse("role=my_role");
  private static final HostSelector FOO_SELECTOR = HostSelector.parse("foo=bar");
  private static final HostSelector BAZ_SELECTOR = HostSelector.parse("baz=qux");

  @Mock
  private MasterModel model;

  private DeploymentGroupResource resource;

  @Before
  public void before() {
    resource = new DeploymentGroupResource(model);
  }

  @Test
  public void testGetNonExistingDeploymentGroup() throws Exception {
    when(model.getDeploymentGroup(anyString())).thenThrow(
        new DeploymentGroupDoesNotExistException(""));

    final Response response = resource.getDeploymentGroup("foobar");
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
  }

  @Test
  public void testGetDeploymentGroup() throws Exception {
    final JobId jobId = JobId.newBuilder()
        .setName("my_job")
        .setVersion("0.2")
        .setHash("1234")
        .build();
    final DeploymentGroup dg = DeploymentGroup.newBuilder()
        .setName("foo")
        .setHostSelectors(ImmutableList.of(ROLE_SELECTOR, FOO_SELECTOR))
        .setJobId(jobId)
        .build();
    when(model.getDeploymentGroup("foo")).thenReturn(dg);

    final Response response = resource.getDeploymentGroup("foo");
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(dg, response.getEntity());
  }

  @Test
  public void testCreateNewDeploymentGroup() {
    final Response response = resource.createDeploymentGroup(mock(DeploymentGroup.class));
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(new CreateDeploymentGroupResponse(
        CreateDeploymentGroupResponse.Status.CREATED), response.getEntity());
  }

  @Test
  public void testCreateExistingSameDeploymentGroup() throws Exception {
    final DeploymentGroup dg = mock(DeploymentGroup.class);
    when(dg.getName()).thenReturn("foo");
    when(dg.getHostSelectors()).thenReturn(Lists.newArrayList(FOO_SELECTOR));
    doThrow(new DeploymentGroupExistsException("")).when(model).addDeploymentGroup(dg);
    when(model.getDeploymentGroup("foo")).thenReturn(dg);

    final Response response = resource.createDeploymentGroup(dg);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(new CreateDeploymentGroupResponse(
        CreateDeploymentGroupResponse.Status.NOT_MODIFIED), response.getEntity());
  }

  @Test
  public void testCreateExistingConflictingDeploymentGroup() throws Exception {
    final DeploymentGroup dg = mock(DeploymentGroup.class);
    when(dg.getName()).thenReturn("foo");
    when(dg.getHostSelectors()).thenReturn(Lists.newArrayList(FOO_SELECTOR));
    doThrow(new DeploymentGroupExistsException("")).when(model).addDeploymentGroup(dg);

    final DeploymentGroup existing = mock(DeploymentGroup.class);
    when(existing.getHostSelectors()).thenReturn(Lists.newArrayList(BAZ_SELECTOR));
    when(model.getDeploymentGroup("foo")).thenReturn(existing);

    final Response response = resource.createDeploymentGroup(dg);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(new CreateDeploymentGroupResponse(
        CreateDeploymentGroupResponse.Status.CONFLICT), response.getEntity());
  }

  @Test
  public void testRemoveDeploymentGroup() throws Exception {
    final Response response = resource.removeDeploymentGroup("foo");
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(new RemoveDeploymentGroupResponse(
        RemoveDeploymentGroupResponse.Status.REMOVED), response.getEntity());
  }

  @Test
  public void testRemoveNonExistingDeploymentGroup() throws Exception {
    doThrow(new DeploymentGroupDoesNotExistException("")).when(model)
        .removeDeploymentGroup(anyString());

    final Response response = resource.removeDeploymentGroup("foo");
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(new RemoveDeploymentGroupResponse(
        RemoveDeploymentGroupResponse.Status.DEPLOYMENT_GROUP_NOT_FOUND), response.getEntity());
  }

  @Test
  public void testRollingUpdateDeploymentGroupDoesNotExist() throws Exception {
    doThrow(new DeploymentGroupDoesNotExistException("")).when(model).rollingUpdate(
        any(DeploymentGroup.class), any(JobId.class), any(RolloutOptions.class));

    final Response response = resource.rollingUpdate(
        "foo", new RollingUpdateRequest(new JobId("foo", "0.3", "1234"),
            RolloutOptions.newBuilder().build()));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(new RollingUpdateResponse(RollingUpdateResponse.Status.DEPLOYMENT_GROUP_NOT_FOUND),
        response.getEntity());
  }

  @Test
  public void testRollingUpdateJobDoesNotExist() throws Exception {
    doThrow(new JobDoesNotExistException("")).when(model).rollingUpdate(
        any(DeploymentGroup.class), any(JobId.class), any(RolloutOptions.class));

    final Response response = resource.rollingUpdate(
        "foo", new RollingUpdateRequest(new JobId("foo", "0.3", "1234"),
            RolloutOptions.newBuilder().build()));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(new RollingUpdateResponse(RollingUpdateResponse.Status.JOB_NOT_FOUND),
        response.getEntity());
  }
}

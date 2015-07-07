/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.helios.master.resources;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.descriptors.JobId;
import com.spotify.helios.common.protocol.CreateDeploymentGroupResponse;
import com.spotify.helios.common.protocol.RemoveDeploymentGroupResponse;
import com.spotify.helios.common.protocol.RollingUpdateRequest;
import com.spotify.helios.common.protocol.RollingUpdateResponse;
import com.spotify.helios.master.DeploymentGroupDoesNotExistException;
import com.spotify.helios.master.DeploymentGroupExistsException;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.MasterModel;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.HashMap;

import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DeploymentGroupResourceTest {

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
    final DeploymentGroup dg = new DeploymentGroup(
        "foo", ImmutableMap.of("role", "my_role", "foo", "bar"),
        new JobId("my_job", "0.2", "1234"), null);
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
    when(dg.getLabels()).thenReturn(ImmutableMap.of("foo", "bar"));
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
    when(dg.getLabels()).thenReturn(ImmutableMap.of("foo", "bar"));
    doThrow(new DeploymentGroupExistsException("")).when(model).addDeploymentGroup(dg);

    final DeploymentGroup existing = mock(DeploymentGroup.class);
    when(existing.getLabels()).thenReturn(ImmutableMap.of("baz", "qux"));
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

  /*
  @Test
  public void testRollingUpdateDeploymentGroupDoesNotExist() throws Exception {
    doThrow(new DeploymentGroupDoesNotExistException(""))
        .when(model).rollingUpdate(anyString(), any(JobId.class));

    final Response response = resource.rollingUpdate(
        "foo", new RollingUpdateRequest(new JobId("foo", "0.3", "1234")));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(new RollingUpdateResponse(RollingUpdateResponse.Status.DEPLOYMENT_GROUP_NOT_FOUND),
                 response.getEntity());
  }

  @Test
  public void testRollingUpdateJobDoesNotExist() throws Exception {
    doThrow(new JobDoesNotExistException(""))
        .when(model).rollingUpdate(anyString(), any(JobId.class));

    final Response response = resource.rollingUpdate(
        "foo", new RollingUpdateRequest(new JobId("foo", "0.3", "1234")));

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals(new RollingUpdateResponse(RollingUpdateResponse.Status.JOB_NOT_FOUND),
                 response.getEntity());
  }
  */
}

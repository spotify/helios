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

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.spotify.helios.common.descriptors.DeploymentGroup;
import com.spotify.helios.common.protocol.RollingUpdateRequest;
import com.spotify.helios.master.DeploymentGroupDoesNotExistException;
import com.spotify.helios.master.DeploymentGroupExistsException;
import com.spotify.helios.master.JobDoesNotExistException;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.master.http.Responses;

import java.net.URI;

import javax.validation.Valid;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;


@Path("/deployment-group")
public class DeploymentGroupResource {

  private final MasterModel model;

  public DeploymentGroupResource(final MasterModel model) {
    this.model = model;
  }


  @POST
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response createDeploymentGroup(@Valid final DeploymentGroup deploymentGroup) {
    try {
      model.addDeploymentGroup(deploymentGroup);
      return Response.created(URI.create(deploymentGroup.getName())).build();
    } catch (DeploymentGroupExistsException ignored) {
      final DeploymentGroup existing;
      try {
        existing = model.getDeploymentGroup(deploymentGroup.getName());
      } catch (DeploymentGroupDoesNotExistException e) {
        // Racy edge condition -- return 500
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
      }

      if (!existing.getLabels().equals(deploymentGroup.getLabels())) {
        return Response.status(Response.Status.CONFLICT).build();
      }

      return Response.created(URI.create(deploymentGroup.getName())).build();
    }
  }

  @GET
  @Path("/{name}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public DeploymentGroup getDeploymentGroup(@PathParam("name") final String name) {
    try {
      final DeploymentGroup dg = model.getDeploymentGroup(name);
      return dg;
    } catch (final DeploymentGroupDoesNotExistException e) {
      throw Responses.notFound();
    }
  }

  @DELETE
  @Path("/{name}")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response removeDeploymentGroup(@PathParam("name") @Valid final String name) {
    try {
      model.removeDeploymentGroup(name);
      return Response.noContent().build();
    } catch (final DeploymentGroupDoesNotExistException e) {
      throw Responses.notFound();
    }
  }

  @POST
  @Path("/{name}/rolling-update")
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public Response rollingUpdate(@PathParam("name") @Valid final String name,
                                @Valid final RollingUpdateRequest args) {
    // TODO(staffan): nicer error messages
    try {
      model.rollingUpdate(name, args.getJob());
    } catch (DeploymentGroupDoesNotExistException e) {
      throw Responses.notFound();
    } catch (JobDoesNotExistException e) {
      throw Responses.badRequest();
    }

    return Response.ok().build();
  }
}

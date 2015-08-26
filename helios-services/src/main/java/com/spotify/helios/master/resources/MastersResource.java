/*
 * Copyright (c) 2014 Spotify AB.
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

import com.google.common.collect.Lists;

import com.codahale.metrics.annotation.ExceptionMetered;
import com.codahale.metrics.annotation.Timed;
import com.spotify.helios.master.MasterModel;
import com.spotify.helios.servicescommon.Output;

import java.util.List;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Path("/masters")
public class MastersResource {

  private final MasterModel model;

  public MastersResource(final MasterModel model) {
    this.model = model;
  }

  /**
   * Returns a list of names of running Helios masters.
   * @return The list of names.
   */
  @GET
  @Produces(APPLICATION_JSON)
  @Timed
  @ExceptionMetered
  public List<String> list(@QueryParam("full") @DefaultValue("0") final String full) {
    final List<String> masters = model.getRunningMasters();
    final boolean fullB = Boolean.parseBoolean(full);
    if (fullB) {
      return masters;
    }
    final List<String> formattedMasters = Lists.newArrayList();
    for (final String master : masters) {
      formattedMasters.add(Output.shortHostname(master));
    }
    return formattedMasters;
  }
}

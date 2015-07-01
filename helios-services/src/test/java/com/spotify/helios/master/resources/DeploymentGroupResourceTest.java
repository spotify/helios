/*
 * Copyright (c) 2015 Spotify AB
 */

package com.spotify.helios.master.resources;

import com.spotify.helios.master.MasterModel;

import org.junit.Test;

import static org.mockito.Mockito.mock;

public class DeploymentGroupResourceTest {

  @Test
  public void testCreateDeploymentGroup() {
    final DeploymentGroupResource resource = new DeploymentGroupResource(mock(MasterModel.class));

    //resource.createDeploymentGroup("foo", new Map)
  }
}

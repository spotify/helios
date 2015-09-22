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

package com.spotify.helios.servicescommon;

import com.spotify.helios.servicescommon.coordination.Paths;
import com.spotify.helios.servicescommon.coordination.ZooKeeperClient;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.UUID;

import static com.google.common.base.Charsets.UTF_8;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ZooKeeperRegistrarUtilTest {

  private static final String HOSTNAME = "host";
  private static final String ID = UUID.randomUUID().toString();

  @Mock ZooKeeperClient zkClient;

  @Test
  public void testRegisterHost() throws Exception {
    final String idPath = Paths.configHostId(HOSTNAME);
    ZooKeeperRegistrarUtil.registerHost(zkClient, idPath, HOSTNAME, ID);
    verify(zkClient).ensurePath(Paths.configHost(HOSTNAME));
    verify(zkClient).ensurePath(Paths.configHostJobs(HOSTNAME));
    verify(zkClient).ensurePath(Paths.configHostPorts(HOSTNAME));
    verify(zkClient).ensurePath(Paths.statusHost(HOSTNAME));
    verify(zkClient).ensurePath(Paths.statusHostJobs(HOSTNAME));
    verify(zkClient).createAndSetData(idPath, ID.getBytes(UTF_8));
  }

}

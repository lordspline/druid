/*
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

package org.apache.robux.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.robux.client.coordinator.Coordinator;
import org.apache.robux.client.coordinator.CoordinatorSelectorConfig;
import org.apache.robux.discovery.RobuxLeaderClient;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.java.util.http.client.HttpClient;

/**
 */
public class CoordinatorDiscoveryModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "robux.selectors.coordinator", CoordinatorSelectorConfig.class);
  }

  @Provides
  @Coordinator
  @ManageLifecycle
  public RobuxLeaderClient getLeaderHttpClient(
      @EscalatedGlobal HttpClient httpClient,
      RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider
  )
  {
    return new RobuxLeaderClient(
        httpClient,
        robuxNodeDiscoveryProvider,
        NodeRole.COORDINATOR,
        "/robux/coordinator/v1/leader"
    );
  }
}

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

package org.apache.robux.k8s.discovery;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import org.apache.robux.client.coordinator.Coordinator;
import org.apache.robux.client.indexing.IndexingService;
import org.apache.robux.discovery.RobuxLeaderSelector;
import org.apache.robux.discovery.RobuxNodeAnnouncer;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.PolyBind;
import org.apache.robux.initialization.RobuxModule;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class K8sDiscoveryModule implements RobuxModule
{
  private static final String K8S_KEY = "k8s";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "robux.discovery.k8s", K8sDiscoveryConfig.class);

    binder.bind(ApiClient.class)
          .toProvider(
              () -> {
                try {
                  // Note: we can probably improve things here about figuring out how to find the K8S API server,
                  // HTTP client timeouts etc.
                  return Config.defaultClient();
                }
                catch (IOException ex) {
                  throw new RuntimeException("Failed to create K8s ApiClient instance", ex);
                }
              }
          )
          .in(LazySingleton.class);

    binder.bind(K8sApiClient.class).to(DefaultK8sApiClient.class).in(LazySingleton.class);
    binder.bind(K8sLeaderElectorFactory.class).to(DefaultK8sLeaderElectorFactory.class).in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(RobuxNodeDiscoveryProvider.class))
            .addBinding(K8S_KEY)
            .to(K8sRobuxNodeDiscoveryProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(RobuxNodeAnnouncer.class))
            .addBinding(K8S_KEY)
            .to(K8sRobuxNodeAnnouncer.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(RobuxLeaderSelector.class, Coordinator.class))
            .addBinding(K8S_KEY)
            .toProvider(
                K8sRobuxLeaderSelectorProvider.K8sCoordinatorRobuxLeaderSelectorProvider.class
            )
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(RobuxLeaderSelector.class, IndexingService.class))
            .addBinding(K8S_KEY)
            .toProvider(
                K8sRobuxLeaderSelectorProvider.K8sIndexingServiceRobuxLeaderSelectorProvider.class
            )
            .in(LazySingleton.class);
  }
}

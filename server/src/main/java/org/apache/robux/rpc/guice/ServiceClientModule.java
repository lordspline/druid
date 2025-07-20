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

package org.apache.robux.rpc.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.robux.client.broker.Broker;
import org.apache.robux.client.broker.BrokerClient;
import org.apache.robux.client.broker.BrokerClientImpl;
import org.apache.robux.client.coordinator.Coordinator;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.client.coordinator.CoordinatorClientImpl;
import org.apache.robux.client.indexing.IndexingService;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.java.util.common.concurrent.ScheduledExecutors;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.rpc.DiscoveryServiceLocator;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.rpc.ServiceClientFactoryImpl;
import org.apache.robux.rpc.ServiceLocator;
import org.apache.robux.rpc.StandardRetryPolicy;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.rpc.indexing.OverlordClientImpl;

import java.util.concurrent.ScheduledExecutorService;

public class ServiceClientModule implements RobuxModule
{
  private static final int CLIENT_MAX_ATTEMPTS = 6;
  private static final int CONNECT_EXEC_THREADS = 4;

  @Override
  public void configure(Binder binder)
  {
    // Nothing to do.
  }

  @Provides
  @LazySingleton
  @EscalatedGlobal
  public ServiceClientFactory getServiceClientFactory(@EscalatedGlobal final HttpClient httpClient)
  {
    return makeServiceClientFactory(httpClient);
  }

  @Provides
  @ManageLifecycle
  @IndexingService
  public ServiceLocator makeOverlordServiceLocator(final RobuxNodeDiscoveryProvider discoveryProvider)
  {
    return new DiscoveryServiceLocator(discoveryProvider, NodeRole.OVERLORD);
  }

  @Provides
  @LazySingleton
  public OverlordClient makeOverlordClient(
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @IndexingService final ServiceLocator serviceLocator
  )
  {
    return new OverlordClientImpl(
        clientFactory.makeClient(
            NodeRole.OVERLORD.getJsonName(),
            serviceLocator,
            StandardRetryPolicy.builder().maxAttempts(CLIENT_MAX_ATTEMPTS).build()
        ),
        jsonMapper
    );
  }

  @Provides
  @ManageLifecycle
  @Coordinator
  public ServiceLocator makeCoordinatorServiceLocator(final RobuxNodeDiscoveryProvider discoveryProvider)
  {
    return new DiscoveryServiceLocator(discoveryProvider, NodeRole.COORDINATOR);
  }

  @Provides
  @LazySingleton
  public CoordinatorClient makeCoordinatorClient(
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Coordinator final ServiceLocator serviceLocator
  )
  {
    return new CoordinatorClientImpl(
        clientFactory.makeClient(
            NodeRole.COORDINATOR.getJsonName(),
            serviceLocator,
            StandardRetryPolicy.builder().maxAttempts(CLIENT_MAX_ATTEMPTS).build()
        ),
        jsonMapper
    );
  }

  @Provides
  @ManageLifecycle
  @Broker
  public ServiceLocator makeBrokerServiceLocator(final RobuxNodeDiscoveryProvider discoveryProvider)
  {
    return new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
  }

  @Provides
  @LazySingleton
  public BrokerClient makeBrokerClient(
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Broker final ServiceLocator serviceLocator
  )
  {
    return new BrokerClientImpl(
        clientFactory.makeClient(
            NodeRole.BROKER.getJsonName(),
            serviceLocator,
            StandardRetryPolicy.builder().maxAttempts(ServiceClientModule.CLIENT_MAX_ATTEMPTS).build()
        ),
        jsonMapper
    );
  }

  public static ServiceClientFactory makeServiceClientFactory(@EscalatedGlobal final HttpClient httpClient)
  {
    final ScheduledExecutorService connectExec =
        ScheduledExecutors.fixed(CONNECT_EXEC_THREADS, "ServiceClientFactory-%d");
    return new ServiceClientFactoryImpl(httpClient, connectExec);
  }
}

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

package org.apache.robux.testing.embedded;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.robux.client.broker.BrokerClient;
import org.apache.robux.client.coordinator.Coordinator;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.client.indexing.IndexingService;
import org.apache.robux.discovery.RobuxLeaderSelector;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.metadata.SQLMetadataConnector;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.metrics.LatchableEmitter;

import java.util.Objects;

/**
 * Holds references to various objects used by an {@link EmbeddedRobuxServer} in
 * embedded cluster tests. The references are for read-only purposes and MUST NOT
 * be mutated in any way.
 */
public final class ServerReferenceHolder implements ServerReferencesProvider
{
  @Inject
  private CoordinatorClient coordinator;

  @Inject
  @Coordinator
  private RobuxLeaderSelector coordinatorLeaderSelector;

  @Inject
  private OverlordClient overlord;

  @Inject
  @IndexingService
  private RobuxLeaderSelector overlordLeaderSelector;

  @Inject
  private BrokerClient broker;

  @Inject(optional = true)
  private LatchableEmitter serviceEmitter;

  @Inject(optional = true)
  private IndexerMetadataStorageCoordinator segmentsMetadataStorage;

  @Inject(optional = true)
  private SQLMetadataConnector sqlMetadataConnector;

  @Inject
  private RobuxNodeDiscoveryProvider nodeDiscoveryProvider;

  @Inject
  @EscalatedGlobal
  private HttpClient httpClient;

  @Self
  @Inject
  private RobuxNode selfNode;

  @Inject
  @Json
  private ObjectMapper jsonMapper;

  @Override
  public RobuxNode selfNode()
  {
    return selfNode;
  }

  @Override
  public CoordinatorClient leaderCoordinator()
  {
    return coordinator;
  }

  @Override
  public RobuxLeaderSelector coordinatorLeaderSelector()
  {
    return coordinatorLeaderSelector;
  }

  @Override
  public OverlordClient leaderOverlord()
  {
    return overlord;
  }

  @Override
  public RobuxLeaderSelector overlordLeaderSelector()
  {
    return overlordLeaderSelector;
  }

  @Override
  public BrokerClient anyBroker()
  {
    return broker;
  }

  @Override
  public LatchableEmitter latchableEmitter()
  {
    return Objects.requireNonNull(serviceEmitter, "LatchableEmitter is not bound");
  }

  @Override
  public IndexerMetadataStorageCoordinator segmentsMetadataStorage()
  {
    return Objects.requireNonNull(segmentsMetadataStorage, "Segment metadata storage is not bound");
  }

  @Override
  public SQLMetadataConnector sqlMetadataConnector()
  {
    return Objects.requireNonNull(sqlMetadataConnector, "SQLMetadataConnector is not bound");
  }

  @Override
  public RobuxNodeDiscoveryProvider nodeDiscovery()
  {
    return nodeDiscoveryProvider;
  }

  @Override
  public HttpClient escalatedHttpClient()
  {
    return httpClient;
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
  }
}

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
import org.apache.robux.client.broker.BrokerClient;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.discovery.RobuxLeaderSelector;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.metadata.SQLMetadataConnector;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.metrics.LatchableEmitter;

/**
 * Provides a handle to the various objects used by an {@link EmbeddedRobuxServer}
 * during an embedded cluster test. The returned references should be used for
 * read-only purposes and MUST NOT be mutated in any way.
 */
public interface ServerReferencesProvider
{
  /**
   * The hostname for this server.
   */
  RobuxNode selfNode();

  /**
   * Client to make API calls to the leader Coordinator in the cluster.
   */
  CoordinatorClient leaderCoordinator();

  /**
   * Leader selector to elect and find the Coordinator leader.
   */
  RobuxLeaderSelector coordinatorLeaderSelector();

  /**
   * Client to make API calls to the leader Overlord in the cluster.
   */
  OverlordClient leaderOverlord();

  /**
   * Leader selector to elect and find the Coordinator leader.
   */
  RobuxLeaderSelector overlordLeaderSelector();

  /**
   * Client to submit queries to any Broker in the cluster.
   */
  BrokerClient anyBroker();

  /**
   * {@link LatchableEmitter} used by this server, if bound.
   */
  LatchableEmitter latchableEmitter();

  /**
   * Metadata storage coordinator to query and update segment metadata directly
   * in the metadata store.
   */
  IndexerMetadataStorageCoordinator segmentsMetadataStorage();

  /**
   * Connector to the SQL-based metadata store.
   */
  SQLMetadataConnector sqlMetadataConnector();

  /**
   * Provider for {@code RobuxNodeDiscovery} for any node type.
   */
  RobuxNodeDiscoveryProvider nodeDiscovery();

  /**
   * {@link HttpClient} used by this server to communicate with other Robux servers.
   */
  HttpClient escalatedHttpClient();

  /**
   * {@link ObjectMapper} annotated with {@link Json}.
   */
  ObjectMapper jsonMapper();
}

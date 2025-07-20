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

package org.apache.robux.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.guice.http.RobuxHttpClientConfig;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.java.util.common.concurrent.ScheduledExecutors;
import org.apache.robux.metadata.TestDerbyConnector;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.timeline.DataSegment;
import org.junit.Rule;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoordinatorSegmentMetadataCacheTestBase extends SegmentMetadataCacheTestBase
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.enabled(true));

  public final ObjectMapper mapper = TestHelper.makeJsonMapper();

  public TestSegmentMetadataQueryWalker walker;
  public TestCoordinatorServerView serverView;
  public List<RobuxServer> robuxServers;
  public SegmentSchemaManager segmentSchemaManager;
  public FingerprintGenerator fingerprintGenerator;
  public SegmentSchemaCache segmentSchemaCache;
  public SegmentSchemaBackFillQueue backFillQueue;

  public void setUp() throws Exception
  {
    setUpData();
    setUpCommon();

    serverView = new TestCoordinatorServerView(
        Lists.newArrayList(segment1, segment2, segment3, segment4, segment5),
        Collections.singletonList(realtimeSegment1)
    );

    Map<SegmentDescriptor, Pair<QueryableIndex, DataSegment>> queryableIndexMap = new HashMap<>();
    queryableIndexMap.put(segment1.toDescriptor(), Pair.of(index1, segment1));
    queryableIndexMap.put(segment2.toDescriptor(), Pair.of(index2, segment2));
    queryableIndexMap.put(segment3.toDescriptor(), Pair.of(index2, segment3));
    queryableIndexMap.put(segment4.toDescriptor(), Pair.of(indexAuto1, segment4));
    queryableIndexMap.put(segment5.toDescriptor(), Pair.of(indexAuto2, segment5));

    walker = new TestSegmentMetadataQueryWalker(
        serverView,
        new RobuxHttpClientConfig()
        {
          @Override
          public long getMaxQueuedBytes()
          {
            return 0L;
          }
        },
        new ServerConfig(),
        new NoopServiceEmitter(),
        conglomerate,
        queryableIndexMap
    );

    robuxServers = serverView.getInventory();

    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createSegmentSchemasTable();
    derbyConnector.createSegmentTable();

    fingerprintGenerator = new FingerprintGenerator(mapper);
    segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        mapper,
        derbyConnector
    );

    segmentSchemaCache = new SegmentSchemaCache();
    CentralizedDatasourceSchemaConfig config = new CentralizedDatasourceSchemaConfig(true, false, 1L, null);

    backFillQueue =
        new SegmentSchemaBackFillQueue(
            segmentSchemaManager,
            ScheduledExecutors::fixed,
            segmentSchemaCache,
            fingerprintGenerator,
            new NoopServiceEmitter(),
            config
        );

    segmentSchemaCache.setInitialized();
  }
}

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

package org.apache.robux.msq.dart.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.client.BrokerServerView;
import org.apache.robux.msq.dart.worker.WorkerId;
import org.apache.robux.msq.exec.MemoryIntrospector;
import org.apache.robux.msq.exec.MemoryIntrospectorImpl;
import org.apache.robux.msq.indexing.LegacyMSQSpec;
import org.apache.robux.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.robux.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.robux.msq.util.MultiStageQueryContext;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryContext;
import org.apache.robux.query.QueryContexts;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.coordination.ServerType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.stream.Collectors;

public class DartControllerContextTest
{
  private static final List<RobuxServerMetadata> SERVERS = ImmutableList.of(
      new RobuxServerMetadata("no", "localhost:1001", null, 1, ServerType.HISTORICAL, "__default", 2), // plaintext
      new RobuxServerMetadata("no", null, "localhost:1002", 1, ServerType.HISTORICAL, "__default", 1), // TLS
      new RobuxServerMetadata("no", "localhost:1003", null, 1, ServerType.REALTIME, "__default", 0)
  );
  private static final RobuxNode SELF_NODE = new RobuxNode("none", "localhost", false, 8080, -1, true, false);
  private static final String QUERY_ID = "abc";

  /**
   * Context returned by {@link #query}. Overrides "maxConcurrentStages".
   */
  private final QueryContext queryContext =
      QueryContext.of(
          ImmutableMap.of(
              MultiStageQueryContext.CTX_MAX_CONCURRENT_STAGES, 3,
              QueryContexts.CTX_DART_QUERY_ID, QUERY_ID
          )
      );
  private MemoryIntrospector memoryIntrospector;
  private AutoCloseable mockCloser;

  /**
   * Server view that returns {@link #SERVERS}.
   */
  @Mock
  private BrokerServerView serverView;

  /**
   * Query spec that exists mainly to test {@link DartControllerContext#queryKernelConfig}.
   */
  @Mock
  private LegacyMSQSpec querySpec;

  /**
   * Query returned by {@link #querySpec}.
   */
  @Mock
  private Query query;

  @BeforeEach
  public void setUp()
  {
    mockCloser = MockitoAnnotations.openMocks(this);
    memoryIntrospector = new MemoryIntrospectorImpl(100_000_000, 0.75, 1, 1, null);
    Mockito.when(serverView.getRobuxServerMetadatas()).thenReturn(SERVERS);
    Mockito.when(querySpec.getDestination()).thenReturn(TaskReportMSQDestination.instance());
    Mockito.when(querySpec.getContext()).thenReturn(queryContext);
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    mockCloser.close();
  }

  @Test
  public void test_queryKernelConfig()
  {
    final DartControllerContext controllerContext =
        new DartControllerContext(null, null, SELF_NODE, null, memoryIntrospector, serverView, null, queryContext);
    final ControllerQueryKernelConfig queryKernelConfig = controllerContext.queryKernelConfig(querySpec);

    Assertions.assertFalse(queryKernelConfig.isFaultTolerant());
    Assertions.assertFalse(queryKernelConfig.isDurableStorage());
    Assertions.assertEquals(3, queryKernelConfig.getMaxConcurrentStages());
    Assertions.assertEquals(TaskReportMSQDestination.instance(), queryKernelConfig.getDestination());
    Assertions.assertTrue(queryKernelConfig.isPipeline());

    // Check workerIds after sorting, because they've been shuffled.
    Assertions.assertEquals(
        ImmutableList.of(
            // Only the HISTORICAL servers
            WorkerId.fromRobuxServerMetadata(SERVERS.get(0), QUERY_ID).toString(),
            WorkerId.fromRobuxServerMetadata(SERVERS.get(1), QUERY_ID).toString()
        ),
        queryKernelConfig.getWorkerIds().stream().sorted().collect(Collectors.toList())
    );
  }
}

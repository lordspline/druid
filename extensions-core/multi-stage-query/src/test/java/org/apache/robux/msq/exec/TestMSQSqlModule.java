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

package org.apache.robux.msq.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Provides;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.TestRobuxModule;
import org.apache.robux.msq.guice.MultiStageQuery;
import org.apache.robux.msq.indexing.destination.SegmentGenerationTerminalStageSpecFactory;
import org.apache.robux.msq.sql.MSQTaskQueryKitSpecFactory;
import org.apache.robux.msq.sql.MSQTaskSqlEngine;
import org.apache.robux.msq.test.MSQTestBase;
import org.apache.robux.msq.test.MSQTestOverlordServiceClient;
import org.apache.robux.msq.test.MSQTestTaskActionClient;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.SqlToolbox;
import org.apache.robux.sql.avatica.RobuxMeta;
import org.apache.robux.sql.avatica.MSQRobuxMeta;

public class TestMSQSqlModule extends TestRobuxModule
{
  @Provides
  @MultiStageQuery
  @LazySingleton
  public SqlStatementFactory makeMSQSqlStatementFactory(
      final MSQTaskSqlEngine sqlEngine,
      SqlToolbox toolbox)
  {
    return new SqlStatementFactory(toolbox.withEngine(sqlEngine));
  }

  @Provides
  @LazySingleton
  public MSQTaskSqlEngine createEngine(
      ObjectMapper queryJsonMapper,
      MSQTestOverlordServiceClient indexingServiceClient,
      MSQTaskQueryKitSpecFactory queryKitSpecFactory)
  {
    return new MSQTaskSqlEngine(indexingServiceClient, queryJsonMapper, new SegmentGenerationTerminalStageSpecFactory(), queryKitSpecFactory, null);
  }

  @Provides
  @LazySingleton
  private MSQTestOverlordServiceClient makeOverlordServiceClient(
      ObjectMapper queryJsonMapper,
      Injector injector,
      WorkerMemoryParameters workerMemoryParameters)
  {
    final MSQTestOverlordServiceClient indexingServiceClient = new MSQTestOverlordServiceClient(
        queryJsonMapper,
        injector,
        new MSQTestTaskActionClient(queryJsonMapper, injector),
        workerMemoryParameters,
        ImmutableList.of()
    );
    return indexingServiceClient;
  }

  @Provides
  @LazySingleton
  private WorkerMemoryParameters makeWorkerMemoryParameters()
  {
    return MSQTestBase.makeTestWorkerMemoryParameters();
  }

  @Provides
  @LazySingleton
  public RobuxMeta createMeta(MSQRobuxMeta robuxMeta)
  {
    return robuxMeta;
  }
}

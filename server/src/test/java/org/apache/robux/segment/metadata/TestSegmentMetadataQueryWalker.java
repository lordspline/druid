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

import org.apache.robux.client.CoordinatorServerView;
import org.apache.robux.guice.http.RobuxHttpClientConfig;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.java.util.common.guava.FunctionalIterable;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.query.BySegmentQueryRunner;
import org.apache.robux.query.DirectQueryProcessingPool;
import org.apache.robux.query.FinalizeResultsQueryRunner;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerFactory;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QueryToolChest;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.QueryableIndexSegment;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.timeline.DataSegment;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TestSegmentMetadataQueryWalker extends SegmentMetadataQuerySegmentWalker
{
  private final Map<SegmentDescriptor, Pair<QueryableIndex, DataSegment>> queryableIndexMap;

  public TestSegmentMetadataQueryWalker(
      CoordinatorServerView serverView,
      RobuxHttpClientConfig httpClientConfig,
      ServerConfig serverConfig,
      ServiceEmitter emitter,
      QueryRunnerFactoryConglomerate conglomerate,
      Map<SegmentDescriptor, Pair<QueryableIndex, DataSegment>> queryableIndexMap
  )
  {
    super(
        serverView,
        httpClientConfig,
        conglomerate,
        serverConfig,
        emitter
    );
    this.queryableIndexMap = queryableIndexMap;
  }

  public void add(DataSegment segment, QueryableIndex index)
  {
    queryableIndexMap.put(segment.toDescriptor(), Pair.of(index, segment));
  }

  @Override
  <T> Sequence getServerResults(
      QueryRunner serverRunner,
      QueryPlus<T> queryPlus,
      ResponseContext responseContext,
      long maxQueuedBytesPerServer,
      List<SegmentDescriptor> segmentDescriptors
  )
  {
    QueryRunnerFactory factory = conglomerate.findFactory(queryPlus.getQuery());
    QueryToolChest toolChest = factory.getToolchest();

    return new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            factory.mergeRunners(
                DirectQueryProcessingPool.INSTANCE,
                FunctionalIterable
                    .create(segmentDescriptors)
                    .transform(
                        segment ->
                            new BySegmentQueryRunner<>(
                                queryableIndexMap.get(segment).rhs.getId(),
                                queryableIndexMap.get(segment).rhs.getInterval().getStart(),
                                factory.createRunner(
                                    new QueryableIndexSegment(
                                        queryableIndexMap.get(segment).lhs,
                                        queryableIndexMap.get(segment).rhs.getId()))
                            )
                    )
            )
        ),
        toolChest
    ).run(queryPlus, responseContext);
  }

  public List<DataSegment> getSegments()
  {
    return queryableIndexMap.values()
                            .stream()
                            .map(pair -> pair.rhs)
                            .collect(Collectors.toList());
  }
}

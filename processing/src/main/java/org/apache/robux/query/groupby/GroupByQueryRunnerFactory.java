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

package org.apache.robux.query.groupby;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.robux.collections.NonBlockingPool;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryProcessingPool;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerFactory;
import org.apache.robux.query.QueryToolChest;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.segment.CursorFactory;
import org.apache.robux.segment.Segment;
import org.apache.robux.segment.TimeBoundaryInspector;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 *
 */
public class GroupByQueryRunnerFactory implements QueryRunnerFactory<ResultRow, GroupByQuery>
{
  private final GroupingEngine groupingEngine;
  private final GroupByQueryQueryToolChest toolChest;
  private final NonBlockingPool<ByteBuffer> processingBufferPool;

  @Inject
  public GroupByQueryRunnerFactory(
      GroupingEngine groupingEngine,
      GroupByQueryQueryToolChest toolChest,
      @Global NonBlockingPool<ByteBuffer> processingBufferPool
  )
  {
    this.groupingEngine = groupingEngine;
    this.toolChest = toolChest;
    this.processingBufferPool = processingBufferPool;
  }

  @Override
  public QueryRunner<ResultRow> createRunner(final Segment segment)
  {
    return new GroupByQueryRunner(segment, groupingEngine, processingBufferPool);
  }

  /**
   * @see GroupingEngine#mergeRunners(QueryProcessingPool, Iterable)
   */
  @Override
  public QueryRunner<ResultRow> mergeRunners(
      final QueryProcessingPool queryProcessingPool,
      final Iterable<QueryRunner<ResultRow>> queryRunners
  )
  {
    return (queryPlus, responseContext) -> {
      QueryRunner<ResultRow> rowQueryRunner = groupingEngine.mergeRunners(queryProcessingPool, queryRunners);
      return rowQueryRunner.run(queryPlus, responseContext);
    };
  }

  @Override
  public QueryToolChest<ResultRow, GroupByQuery> getToolchest()
  {
    return toolChest;
  }

  private static class GroupByQueryRunner implements QueryRunner<ResultRow>
  {
    private final CursorFactory cursorFactory;
    @Nullable
    private final TimeBoundaryInspector timeBoundaryInspector;
    private final GroupingEngine groupingEngine;
    private final NonBlockingPool<ByteBuffer> processingBufferPool;

    public GroupByQueryRunner(
        Segment segment,
        final GroupingEngine groupingEngine,
        final NonBlockingPool<ByteBuffer> processingBufferPool
    )
    {
      this.cursorFactory = Objects.requireNonNull(segment.as(CursorFactory.class));
      this.timeBoundaryInspector = segment.as(TimeBoundaryInspector.class);
      this.groupingEngine = groupingEngine;
      this.processingBufferPool = processingBufferPool;
    }

    @Override
    public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
    {
      Query<ResultRow> query = queryPlus.getQuery();
      if (!(query instanceof GroupByQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", query.getClass(), GroupByQuery.class);
      }

      return groupingEngine.process(
          (GroupByQuery) query,
          cursorFactory,
          timeBoundaryInspector,
          processingBufferPool,
          (GroupByQueryMetrics) queryPlus.getQueryMetrics()
      );
    }
  }

  @VisibleForTesting
  public GroupingEngine getGroupingEngine()
  {
    return groupingEngine;
  }
}

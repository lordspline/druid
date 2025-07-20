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

package org.apache.robux.query.topn;

import com.google.inject.Inject;
import org.apache.robux.collections.NonBlockingPool;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.query.ChainedExecutionQueryRunner;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryProcessingPool;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerFactory;
import org.apache.robux.query.QueryToolChest;
import org.apache.robux.query.QueryWatcher;
import org.apache.robux.query.Result;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.segment.Segment;

import java.nio.ByteBuffer;

/**
 *
 */
public class TopNQueryRunnerFactory implements QueryRunnerFactory<Result<TopNResultValue>, TopNQuery>
{
  private final NonBlockingPool<ByteBuffer> computationBufferPool;
  private final TopNQueryQueryToolChest toolchest;
  private final QueryWatcher queryWatcher;

  @Inject
  public TopNQueryRunnerFactory(
      @Global NonBlockingPool<ByteBuffer> computationBufferPool,
      TopNQueryQueryToolChest toolchest,
      QueryWatcher queryWatcher
  )
  {
    this.computationBufferPool = computationBufferPool;
    this.toolchest = toolchest;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> createRunner(final Segment segment)
  {
    final TopNQueryEngine queryEngine = new TopNQueryEngine(computationBufferPool);
    return new QueryRunner<>()
    {
      @Override
      public Sequence<Result<TopNResultValue>> run(
          QueryPlus<Result<TopNResultValue>> input,
          ResponseContext responseContext
      )
      {
        if (!(input.getQuery() instanceof TopNQuery)) {
          throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TopNQuery.class);
        }

        TopNQuery query = (TopNQuery) input.getQuery();
        return queryEngine.query(
            query,
            segment,
            (TopNQueryMetrics) input.getQueryMetrics()
        );
      }
    };

  }

  @Override
  public QueryRunner<Result<TopNResultValue>> mergeRunners(
      QueryProcessingPool queryProcessingPool,
      Iterable<QueryRunner<Result<TopNResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<>(queryProcessingPool, queryWatcher, queryRunners);
  }

  @Override
  public QueryToolChest<Result<TopNResultValue>, TopNQuery> getToolchest()
  {
    return toolchest;
  }
}

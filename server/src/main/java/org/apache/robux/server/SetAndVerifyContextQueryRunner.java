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

package org.apache.robux.server;

import com.google.common.collect.ImmutableMap;
import org.apache.robux.client.DirectRobuxClient;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.query.Queries;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryContext;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.server.initialization.ServerConfig;

/**
 * Use this QueryRunner to set and verify Query contexts.
 */
public class SetAndVerifyContextQueryRunner<T> implements QueryRunner<T>
{
  private final ServerConfig serverConfig;
  private final QueryRunner<T> baseRunner;
  private final long startTimeMillis;

  public SetAndVerifyContextQueryRunner(ServerConfig serverConfig, QueryRunner<T> baseRunner)
  {
    this.serverConfig = serverConfig;
    this.baseRunner = baseRunner;
    this.startTimeMillis = System.currentTimeMillis();
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    return baseRunner.run(
        QueryPlus.wrap(withTimeoutAndMaxScatterGatherBytes(queryPlus.getQuery(), serverConfig)),
        responseContext
    );
  }

  public Query<T> withTimeoutAndMaxScatterGatherBytes(Query<T> query, ServerConfig serverConfig)
  {
    Query<T> newQuery =
        Queries.withMaxScatterGatherBytes(
            Queries.withDefaultTimeout(
                query,
                Math.min(serverConfig.getDefaultQueryTimeout(), serverConfig.getMaxQueryTimeout())
            ),
            serverConfig.getMaxScatterGatherBytes()
        );
    newQuery.context().verifyMaxQueryTimeout(
        serverConfig.getMaxQueryTimeout()
    );
    // DirectRobuxClient.QUERY_FAIL_TIME is used by DirectRobuxClient and JsonParserIterator to determine when to
    // fail with a timeout exception
    final long failTime;
    final QueryContext context = newQuery.context();
    if (context.hasTimeout()) {
      failTime = this.startTimeMillis + context.getTimeout();
    } else {
      failTime = this.startTimeMillis + serverConfig.getMaxQueryTimeout();
    }
    return newQuery.withOverriddenContext(
        ImmutableMap.of(DirectRobuxClient.QUERY_FAIL_TIME, failTime > 0 ? failTime : Long.MAX_VALUE)
    );
  }
}

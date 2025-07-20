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

package org.apache.robux.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.annotations.EscalatedClient;
import org.apache.robux.guice.annotations.Smile;
import org.apache.robux.java.util.common.concurrent.ScheduledExecutors;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QueryWatcher;
import org.apache.robux.utils.JvmUtils;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for building {@link DirectRobuxClient}
 */
@LazySingleton
public class DirectRobuxClientFactory implements QueryableRobuxServer.Maker
{
  private final ServiceEmitter emitter;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final ScheduledExecutorService queryCancellationExecutor;

  @Inject
  public DirectRobuxClientFactory(
      final ServiceEmitter emitter,
      final QueryRunnerFactoryConglomerate conglomerate,
      final QueryWatcher queryWatcher,
      final @Smile ObjectMapper smileMapper,
      final @EscalatedClient HttpClient httpClient
  )
  {
    this.emitter = emitter;
    this.conglomerate = conglomerate;
    this.queryWatcher = queryWatcher;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;

    int threadCount = Math.max(1, JvmUtils.getRuntimeInfo().getAvailableProcessors() / 2);
    this.queryCancellationExecutor = ScheduledExecutors.fixed(threadCount, "query-cancellation-executor");
  }

  public DirectRobuxClient makeDirectClient(RobuxServer server)
  {
    return new DirectRobuxClient(
        conglomerate,
        queryWatcher,
        smileMapper,
        httpClient,
        server.getScheme(),
        server.getHost(),
        emitter,
        queryCancellationExecutor
    );
  }

  @Override
  public QueryableRobuxServer make(RobuxServer server)
  {
    return new QueryableRobuxServer(server, makeDirectClient(server));
  }
}

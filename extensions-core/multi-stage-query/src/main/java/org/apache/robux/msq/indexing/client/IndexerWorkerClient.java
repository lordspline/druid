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

package org.apache.robux.msq.indexing.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.msq.indexing.MSQWorkerTask;
import org.apache.robux.msq.rpc.BaseWorkerClientImpl;
import org.apache.robux.rpc.ServiceClient;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.rpc.StandardRetryPolicy;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.rpc.indexing.SpecificTaskRetryPolicy;
import org.apache.robux.rpc.indexing.SpecificTaskServiceLocator;
import org.apache.robux.utils.CloseableUtils;

import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Worker client for {@link MSQWorkerTask}.
 */
public class IndexerWorkerClient extends BaseWorkerClientImpl
{
  private final ServiceClientFactory clientFactory;
  private final OverlordClient overlordClient;

  @GuardedBy("clientMap")
  private final Map<String, Pair<ServiceClient, Closeable>> clientMap = new HashMap<>();

  public IndexerWorkerClient(
      final ServiceClientFactory clientFactory,
      final OverlordClient overlordClient,
      final ObjectMapper jsonMapper
  )
  {
    super(jsonMapper, MediaType.APPLICATION_JSON);
    this.clientFactory = clientFactory;
    this.overlordClient = overlordClient;
  }

  @Override
  public void close() throws IOException
  {
    synchronized (clientMap) {
      try {
        final List<Closeable> closeables =
            clientMap.values().stream().map(pair -> pair.rhs).collect(Collectors.toList());
        CloseableUtils.closeAll(closeables);
      }
      finally {
        clientMap.clear();
      }
    }
  }

  @Override
  protected ServiceClient getClient(final String workerId)
  {
    synchronized (clientMap) {
      return clientMap.computeIfAbsent(
          workerId,
          id -> {
            final SpecificTaskServiceLocator locator = new SpecificTaskServiceLocator(id, overlordClient);
            final ServiceClient client = clientFactory.makeClient(
                id,
                locator,
                new SpecificTaskRetryPolicy(workerId, StandardRetryPolicy.unlimitedWithoutRetryLogging())
            );
            return Pair.of(client, locator);
          }
      ).lhs;
    }
  }
}

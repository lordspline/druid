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

package org.apache.robux.testing.cluster.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.robux.client.coordinator.Coordinator;
import org.apache.robux.client.coordinator.CoordinatorClientImpl;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.java.util.common.Stopwatch;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.rpc.ServiceLocator;
import org.apache.robux.rpc.StandardRetryPolicy;
import org.apache.robux.testing.cluster.ClusterTestingTaskConfig;
import org.joda.time.Duration;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of {@code CoordinatorClient} that supports the following:
 * <ul>
 * <li>Add a {@code minSegmentHandoffDelay} before making a call to Coordinator
 * to get the handoff status of a single segment.</li>
 * </ul>
 */
public class FaultyCoordinatorClient extends CoordinatorClientImpl
{
  private static final Logger log = new Logger(FaultyCoordinatorClient.class);

  private final Provider<ClusterTestingTaskConfig> testConfigProvider;
  private final ConcurrentHashMap<SegmentDescriptor, Stopwatch> segmentHandoffTimers = new ConcurrentHashMap<>();

  @Inject
  public FaultyCoordinatorClient(
      Provider<ClusterTestingTaskConfig> testingConfigProvider,
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Coordinator final ServiceLocator serviceLocator
  )
  {
    super(
        clientFactory.makeClient(
            NodeRole.COORDINATOR.getJsonName(),
            serviceLocator,
            StandardRetryPolicy.builder().maxAttempts(6).build()
        ),
        jsonMapper
    );
    this.testConfigProvider = testingConfigProvider;
  }

  @Override
  public ListenableFuture<Boolean> isHandoffComplete(String dataSource, SegmentDescriptor descriptor)
  {
    final Duration minHandoffDelay = getHandoffDelay();
    if (minHandoffDelay != null) {
      final Stopwatch sinceHandoffCheckStarted = segmentHandoffTimers.computeIfAbsent(
          descriptor,
          d -> Stopwatch.createStarted()
      );

      if (sinceHandoffCheckStarted.hasElapsed(minHandoffDelay)) {
        // Wait period is over, check with Coordinator now, but do not remove
        // the stopwatch from the map. This ensures that we do not create a new
        // Stopwatch causing further delays.
        log.info(
            "Min handoff delay[%s] has elapsed for segment[%s]. Checking with Coordinator for actual handoff status.",
            minHandoffDelay, descriptor
        );
      } else {
        // Until the min handoff delay has elapsed, keep returning false
        return Futures.immediateFuture(false);
      }
    }

    // Call Coordinator for the actual handoff status
    return super.isHandoffComplete(dataSource, descriptor);
  }

  private Duration getHandoffDelay()
  {
    return testConfigProvider.get().getCoordinatorClientConfig().getMinSegmentHandoffDelay();
  }
}

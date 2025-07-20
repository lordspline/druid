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

package org.apache.robux.server.coordinator.duty;

import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.ImmutableRobuxServer;
import org.apache.robux.client.ServerInventoryView;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.server.coordinator.CoordinatorDynamicConfig;
import org.apache.robux.server.coordinator.RobuxCluster;
import org.apache.robux.server.coordinator.RobuxCoordinatorRuntimeParams;
import org.apache.robux.server.coordinator.ServerHolder;
import org.apache.robux.server.coordinator.balancer.BalancerStrategy;
import org.apache.robux.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.robux.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.robux.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.robux.server.coordinator.loading.SegmentLoadingConfig;
import org.apache.robux.server.coordinator.stats.CoordinatorRunStats;
import org.apache.robux.server.coordinator.stats.Dimension;
import org.apache.robux.server.coordinator.stats.RowKey;
import org.apache.robux.server.coordinator.stats.Stats;
import org.apache.robux.timeline.DataSegment;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This duty does the following:
 * <ul>
 *   <li>Creates an immutable {@link RobuxCluster} consisting of {@link ServerHolder}s
 *   which represent the current state of the servers in the cluster.</li>
 *   <li>Starts and stops load peons for new and disappeared servers respectively.</li>
 *   <li>Cancels in-progress loads on all decommissioning servers. This is done
 *   here to ensure that under-replicated segments are assigned to active servers
 *   in the {@link RunRules} duty after this.</li>
 *   <li>Initializes the {@link BalancerStrategy} for the run.</li>
 * </ul>
 */
public class PrepareBalancerAndLoadQueues implements CoordinatorDuty
{
  private static final Logger log = new Logger(PrepareBalancerAndLoadQueues.class);

  private final LoadQueueTaskMaster taskMaster;
  private final SegmentLoadQueueManager loadQueueManager;
  private final ServerInventoryView serverInventoryView;
  private final BalancerStrategyFactory balancerStrategyFactory;

  public PrepareBalancerAndLoadQueues(
      LoadQueueTaskMaster taskMaster,
      SegmentLoadQueueManager loadQueueManager,
      BalancerStrategyFactory balancerStrategyFactory,
      ServerInventoryView serverInventoryView
  )
  {
    this.taskMaster = taskMaster;
    this.loadQueueManager = loadQueueManager;
    this.balancerStrategyFactory = balancerStrategyFactory;
    this.serverInventoryView = serverInventoryView;
  }

  @Override
  public RobuxCoordinatorRuntimeParams run(RobuxCoordinatorRuntimeParams params)
  {
    List<ImmutableRobuxServer> currentServers = prepareCurrentServers();
    taskMaster.resetPeonsForNewServers(currentServers);

    final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
    final SegmentLoadingConfig segmentLoadingConfig
        = SegmentLoadingConfig.create(dynamicConfig, params.getUsedSegmentCount());

    final RobuxCluster cluster = prepareCluster(dynamicConfig, segmentLoadingConfig, currentServers);
    cancelLoadsOnDecommissioningServers(cluster);

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    collectHistoricalStats(cluster, stats);
    collectUsedSegmentStats(params, stats);
    collectDebugStats(segmentLoadingConfig, stats);

    final int numBalancerThreads = segmentLoadingConfig.getBalancerComputeThreads();
    final BalancerStrategy balancerStrategy = balancerStrategyFactory.createBalancerStrategy(numBalancerThreads);
    log.debug(
        "Using balancer strategy[%s] with [%d] threads.",
        balancerStrategy.getClass().getSimpleName(), numBalancerThreads
    );

    return params.buildFromExisting()
                 .withRobuxCluster(cluster)
                 .withBalancerStrategy(balancerStrategy)
                 .withSegmentLoadingConfig(segmentLoadingConfig)
                 .withSegmentAssignerUsing(loadQueueManager)
                 .build();
  }

  /**
   * Cancels all load/move operations on decommissioning servers. This should
   * be done before initializing the SegmentReplicantLookup so that
   * under-replicated segments can be assigned in the current run itself.
   */
  private void cancelLoadsOnDecommissioningServers(RobuxCluster cluster)
  {
    final AtomicInteger cancelledCount = new AtomicInteger(0);
    final List<ServerHolder> decommissioningServers
        = cluster.getAllManagedServers().stream()
                 .filter(ServerHolder::isDecommissioning)
                 .collect(Collectors.toList());

    for (ServerHolder server : decommissioningServers) {
      server.getQueuedSegments().forEach(
          (segment, action) -> {
            // Cancel the operation if it is a type of load
            if (action.isLoad() && server.cancelOperation(action, segment)) {
              cancelledCount.incrementAndGet();
            }
          }
      );
    }
  }

  private List<ImmutableRobuxServer> prepareCurrentServers()
  {
    return serverInventoryView
        .getInventory()
        .stream()
        .filter(RobuxServer::isSegmentReplicationOrBroadcastTarget)
        .map(RobuxServer::toImmutableRobuxServer)
        .collect(Collectors.toList());
  }

  private RobuxCluster prepareCluster(
      CoordinatorDynamicConfig dynamicConfig,
      SegmentLoadingConfig segmentLoadingConfig,
      List<ImmutableRobuxServer> currentServers
  )
  {
    final Set<String> decommissioningServers = dynamicConfig.getDecommissioningNodes();
    final Set<String> unmanagedServers = new HashSet<>(dynamicConfig.getCloneServers().keySet());
    final RobuxCluster.Builder cluster = RobuxCluster.builder();
    for (ImmutableRobuxServer server : currentServers) {
      cluster.add(
          new ServerHolder(
              server,
              taskMaster.getPeonForServer(server),
              decommissioningServers.contains(server.getHost()),
              unmanagedServers.contains(server.getHost()),
              segmentLoadingConfig.getMaxSegmentsInLoadQueue(),
              segmentLoadingConfig.getMaxLifetimeInLoadQueue()
          )
      );
    }
    return cluster.build();
  }

  private void collectHistoricalStats(RobuxCluster cluster, CoordinatorRunStats stats)
  {
    cluster.getHistoricals().forEach((tier, historicals) -> {
      RowKey rowKey = RowKey.of(Dimension.TIER, tier);
      stats.add(Stats.Tier.HISTORICAL_COUNT, rowKey, historicals.size());

      long totalCapacity = 0;
      long cloneCount = 0;
      for (ServerHolder holder : historicals) {
        if (holder.isUnmanaged()) {
          cloneCount += 1;
        } else {
          totalCapacity += holder.getMaxSize();
        }
      }
      stats.add(Stats.Tier.CLONE_COUNT, rowKey, cloneCount);
      stats.add(Stats.Tier.TOTAL_CAPACITY, rowKey, totalCapacity);
    });
  }

  private void collectUsedSegmentStats(RobuxCoordinatorRuntimeParams params, CoordinatorRunStats stats)
  {
    params.getUsedSegmentsTimelinesPerDataSource().forEach((dataSource, timeline) -> {
      long totalSizeOfUsedSegments = timeline.iterateAllObjects().stream()
                                             .mapToLong(DataSegment::getSize).sum();

      RowKey datasourceKey = RowKey.of(Dimension.DATASOURCE, dataSource);
      stats.add(Stats.Segments.USED_BYTES, datasourceKey, totalSizeOfUsedSegments);
      stats.add(Stats.Segments.USED, datasourceKey, timeline.getNumObjects());
    });
  }

  private void collectDebugStats(SegmentLoadingConfig config, CoordinatorRunStats stats)
  {
    stats.add(Stats.Balancer.COMPUTE_THREADS, config.getBalancerComputeThreads());
    stats.add(Stats.Segments.REPLICATION_THROTTLE_LIMIT, config.getReplicationThrottleLimit());
  }
}

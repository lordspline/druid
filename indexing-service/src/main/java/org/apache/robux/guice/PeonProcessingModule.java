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

package org.apache.robux.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.client.cache.CachePopulator;
import org.apache.robux.client.cache.CachePopulatorStats;
import org.apache.robux.collections.BlockingPool;
import org.apache.robux.collections.DummyBlockingPool;
import org.apache.robux.collections.DummyNonBlockingPool;
import org.apache.robux.collections.NonBlockingPool;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.guice.annotations.Merging;
import org.apache.robux.guice.annotations.Smile;
import org.apache.robux.indexing.common.task.Task;
import org.apache.robux.java.util.common.lifecycle.Lifecycle;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.ExecutorServiceMonitor;
import org.apache.robux.query.NoopQueryProcessingPool;
import org.apache.robux.query.QueryProcessingPool;
import org.apache.robux.query.groupby.GroupByQueryConfig;
import org.apache.robux.query.groupby.GroupByResourcesReservationPool;
import org.apache.robux.utils.RuntimeInfo;

import java.nio.ByteBuffer;

/**
 * This module fulfills the dependency injection of query processing and caching resources: buffer pools and
 * thread pools on Peon selectively. Only the peons for the tasks supporting queries need to allocate direct buffers
 * and thread pools. Thus, this is separate from the {@link RobuxProcessingModule} to separate the needs of the peons and
 * the historicals
 *
 * @see RobuxProcessingModule
 */
public class PeonProcessingModule implements Module
{
  private static final Logger log = new Logger(PeonProcessingModule.class);

  @Override
  public void configure(Binder binder)
  {
    RobuxProcessingModule.registerConfigsAndMonitor(binder);
  }

  @Provides
  @LazySingleton
  public CachePopulator getCachePopulator(
      @Smile ObjectMapper smileMapper,
      CachePopulatorStats cachePopulatorStats,
      CacheConfig cacheConfig
  )
  {
    return RobuxProcessingModule.createCachePopulator(smileMapper, cachePopulatorStats, cacheConfig);
  }

  @Provides
  @ManageLifecycle
  public QueryProcessingPool getProcessingExecutorPool(
      Task task,
      RobuxProcessingConfig config,
      ExecutorServiceMonitor executorServiceMonitor,
      Lifecycle lifecycle
  )
  {
    if (task.supportsQueries()) {
      return RobuxProcessingModule.createProcessingExecutorPool(config, executorServiceMonitor, lifecycle);
    } else {
      if (config.isNumThreadsConfigured()) {
        log.warn(
            "Ignoring the configured numThreads[%d] because task[%s] of type[%s] does not support queries",
            config.getNumThreads(),
            task.getId(),
            task.getType()
        );
      }
      return NoopQueryProcessingPool.instance();
    }
  }

  @Provides
  @LazySingleton
  @Global
  public NonBlockingPool<ByteBuffer> getIntermediateResultsPool(
      Task task,
      RobuxProcessingConfig config,
      RuntimeInfo runtimeInfo
  )
  {
    if (task.supportsQueries()) {
      return RobuxProcessingModule.createIntermediateResultsPool(config, runtimeInfo);
    } else {
      return DummyNonBlockingPool.instance();
    }
  }

  @Provides
  @LazySingleton
  @Merging
  public BlockingPool<ByteBuffer> getMergeBufferPool(Task task, RobuxProcessingConfig config, RuntimeInfo runtimeInfo)
  {
    if (task.supportsQueries()) {
      return RobuxProcessingModule.createMergeBufferPool(config, runtimeInfo);
    } else {
      if (config.isNumMergeBuffersConfigured()) {
        log.warn(
            "Ignoring the configured numMergeBuffers[%d] because task[%s] of type[%s] does not support queries",
            config.getNumThreads(),
            task.getId(),
            task.getType()
        );
      }
      return DummyBlockingPool.instance();
    }
  }

  @Provides
  @LazySingleton
  @Merging
  public GroupByResourcesReservationPool getGroupByResourcesReservationPool(
      @Merging BlockingPool<ByteBuffer> mergeBufferPool,
      GroupByQueryConfig groupByQueryConfig
  )
  {
    return new GroupByResourcesReservationPool(mergeBufferPool, groupByQueryConfig);
  }
}

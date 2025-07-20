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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import org.apache.robux.client.cache.BackgroundCachePopulator;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.client.cache.CachePopulator;
import org.apache.robux.client.cache.CachePopulatorStats;
import org.apache.robux.client.cache.ForegroundCachePopulator;
import org.apache.robux.collections.BlockingPool;
import org.apache.robux.collections.DefaultBlockingPool;
import org.apache.robux.collections.NonBlockingPool;
import org.apache.robux.collections.StupidPool;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.guice.annotations.Merging;
import org.apache.robux.guice.annotations.Smile;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.lifecycle.Lifecycle;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.offheap.OffheapBufferGenerator;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.ExecutorServiceMonitor;
import org.apache.robux.query.MetricsEmittingQueryProcessingPool;
import org.apache.robux.query.PrioritizedExecutorService;
import org.apache.robux.query.QueryProcessingPool;
import org.apache.robux.query.groupby.GroupByQueryConfig;
import org.apache.robux.query.groupby.GroupByResourcesReservationPool;
import org.apache.robux.server.metrics.MetricsModule;
import org.apache.robux.utils.RuntimeInfo;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class RobuxProcessingModule implements Module
{
  public static final String PROCESSING_PROPERTY_PREFIX = "robux.processing";

  private static final Logger log = new Logger(RobuxProcessingModule.class);

  @Override
  public void configure(Binder binder)
  {
    registerConfigsAndMonitor(binder);
  }

  @Provides
  @LazySingleton
  public CachePopulator getCachePopulator(
      @Smile ObjectMapper smileMapper,
      CachePopulatorStats cachePopulatorStats,
      CacheConfig cacheConfig
  )
  {
    return createCachePopulator(smileMapper, cachePopulatorStats, cacheConfig);
  }

  @Provides
  @ManageLifecycle
  public QueryProcessingPool getProcessingExecutorPool(
      RobuxProcessingConfig config,
      ExecutorServiceMonitor executorServiceMonitor,
      Lifecycle lifecycle
  )
  {
    return createProcessingExecutorPool(config, executorServiceMonitor, lifecycle);
  }

  @Provides
  @LazySingleton
  @Global
  public NonBlockingPool<ByteBuffer> getIntermediateResultsPool(RobuxProcessingConfig config, RuntimeInfo runtimeInfo)
  {
    return createIntermediateResultsPool(config, runtimeInfo);
  }

  @Provides
  @LazySingleton
  @Merging
  public BlockingPool<ByteBuffer> getMergeBufferPool(RobuxProcessingConfig config, RuntimeInfo runtimeInfo)
  {
    return createMergeBufferPool(config, runtimeInfo);
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

  public static void registerConfigsAndMonitor(Binder binder)
  {
    JsonConfigProvider.bind(binder, PROCESSING_PROPERTY_PREFIX, RobuxProcessingConfig.class);
    MetricsModule.register(binder, ExecutorServiceMonitor.class);
  }

  public static CachePopulator createCachePopulator(
      ObjectMapper smileMapper,
      CachePopulatorStats cachePopulatorStats,
      CacheConfig cacheConfig
  )
  {
    if (cacheConfig.getNumBackgroundThreads() > 0) {
      final ExecutorService exec = Executors.newFixedThreadPool(
          cacheConfig.getNumBackgroundThreads(),
          new ThreadFactoryBuilder()
              .setNameFormat("background-cacher-%d")
              .setDaemon(true)
              .setPriority(Thread.MIN_PRIORITY)
              .build()
      );

      return new BackgroundCachePopulator(exec, smileMapper, cachePopulatorStats, cacheConfig.getMaxEntrySize());
    } else {
      return new ForegroundCachePopulator(smileMapper, cachePopulatorStats, cacheConfig.getMaxEntrySize());
    }
  }

  public static QueryProcessingPool createProcessingExecutorPool(
      RobuxProcessingConfig config,
      ExecutorServiceMonitor executorServiceMonitor,
      Lifecycle lifecycle
  )
  {
    return new MetricsEmittingQueryProcessingPool(
        PrioritizedExecutorService.create(
            lifecycle,
            config
        ),
        executorServiceMonitor
    );
  }

  public static NonBlockingPool<ByteBuffer> createIntermediateResultsPool(
      final RobuxProcessingConfig config,
      final RuntimeInfo runtimeInfo
  )
  {
    verifyDirectMemory(config, runtimeInfo);
    return new StupidPool<>(
        "intermediate processing pool",
        new OffheapBufferGenerator("intermediate processing", config.intermediateComputeSizeBytes()),
        config.getNumThreads(),
        config.poolCacheMaxCount()
    );
  }

  public static BlockingPool<ByteBuffer> createMergeBufferPool(
      final RobuxProcessingConfig config,
      final RuntimeInfo runtimeInfo
  )
  {
    verifyDirectMemory(config, runtimeInfo);
    return new DefaultBlockingPool<>(
        new OffheapBufferGenerator("result merging", config.intermediateComputeSizeBytes()),
        config.getNumMergeBuffers()
    );
  }

  private static void verifyDirectMemory(final RobuxProcessingConfig config, final RuntimeInfo runtimeInfo)
  {
    try {
      final long maxDirectMemory = runtimeInfo.getDirectMemorySizeBytes();
      final long memoryNeeded = (long) config.intermediateComputeSizeBytes() *
                                (config.getNumMergeBuffers() + config.getNumThreads() + 1);

      if (maxDirectMemory < memoryNeeded) {
        throw new ProvisionException(
            StringUtils.format(
                "Not enough direct memory.  Please adjust -XX:MaxDirectMemorySize, robux.processing.buffer.sizeBytes, robux.processing.numThreads, or robux.processing.numMergeBuffers: "
                + "maxDirectMemory[%,d], memoryNeeded[%,d] = robux.processing.buffer.sizeBytes[%,d] * (robux.processing.numMergeBuffers[%,d] + robux.processing.numThreads[%,d] + 1)",
                maxDirectMemory,
                memoryNeeded,
                config.intermediateComputeSizeBytes(),
                config.getNumMergeBuffers(),
                config.getNumThreads()
            )
        );
      }
    }
    catch (UnsupportedOperationException e) {
      log.debug("Checking for direct memory size is not support on this platform: %s", e);
      log.info(
          "Unable to determine max direct memory size. If robux.processing.buffer.sizeBytes is explicitly configured, "
          + "then make sure to set -XX:MaxDirectMemorySize to at least \"robux.processing.buffer.sizeBytes * "
          + "(robux.processing.numMergeBuffers[%,d] + robux.processing.numThreads[%,d] + 1)\", "
          + "or else set -XX:MaxDirectMemorySize to at least 25%% of maximum jvm heap size.",
          config.getNumMergeBuffers(),
          config.getNumThreads()
      );
    }
  }
}

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

package org.apache.robux.msq.guice;

import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.guice.annotations.LoadScope;
import org.apache.robux.indexing.worker.config.WorkerConfig;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.msq.exec.MemoryIntrospector;
import org.apache.robux.msq.exec.MemoryIntrospectorImpl;
import org.apache.robux.msq.exec.ProcessingBuffersProvider;
import org.apache.robux.msq.indexing.IndexerProcessingBuffersProvider;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.robux.utils.RuntimeInfo;

/**
 * Provides {@link MemoryIntrospector} for multi-task-per-JVM model.
 *
 * @see PeonMemoryManagementModule for single-task-per-JVM model used on {@link org.apache.robux.cli.CliPeon}
 */
@LoadScope(roles = NodeRole.INDEXER_JSON_NAME)
public class IndexerMemoryManagementModule implements RobuxModule
{
  /**
   * Allocate up to 60% of memory for the MSQ framework (if all running tasks are MSQ tasks). This does not include the
   * memory allocated to {@link #PROCESSING_MEMORY_FRACTION}.
   */
  private static final double MSQ_MEMORY_FRACTION = 0.60;

  /**
   * Allocate up to 15% of memory for processing buffers for MSQ tasks.
   */
  private static final double PROCESSING_MEMORY_FRACTION = 0.15;

  @Override
  public void configure(Binder binder)
  {
    TaskMemoryManagementConfig.bind(binder);
  }

  @Provides
  @ManageLifecycle
  public MemoryIntrospector createMemoryIntrospector(
      final RuntimeInfo runtimeInfo,
      final LookupExtractorFactoryContainerProvider lookupProvider,
      final TaskMemoryManagementConfig taskMemoryManagementConfig,
      final RobuxProcessingConfig processingConfig,
      final WorkerConfig workerConfig
  )
  {
    return new MemoryIntrospectorImpl(
        runtimeInfo.getMaxHeapSizeBytes(),
        MSQ_MEMORY_FRACTION,
        workerConfig.getCapacity(),
        PeonMemoryManagementModule.getNumThreads(taskMemoryManagementConfig, processingConfig),
        lookupProvider
    );
  }

  @Provides
  @LazySingleton
  public ProcessingBuffersProvider createProcessingBuffersProvider(
      final MemoryIntrospector memoryIntrospector,
      final WorkerConfig workerConfig,
      final RuntimeInfo runtimeInfo
  )
  {
    return new IndexerProcessingBuffersProvider(
        (long) (runtimeInfo.getMaxHeapSizeBytes() * PROCESSING_MEMORY_FRACTION),
        workerConfig.getCapacity(),
        memoryIntrospector.numProcessingThreads()
    );
  }
}

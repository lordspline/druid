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

package org.apache.robux.msq.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.robux.collections.ResourceHolder;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Smile;
import org.apache.robux.indexing.common.SegmentCacheManagerFactory;
import org.apache.robux.indexing.common.TaskToolbox;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.msq.exec.ControllerClient;
import org.apache.robux.msq.exec.DataServerQueryHandlerFactory;
import org.apache.robux.msq.exec.FrameContext;
import org.apache.robux.msq.exec.FrameWriterSpec;
import org.apache.robux.msq.exec.MSQMetriceEventBuilder;
import org.apache.robux.msq.exec.MemoryIntrospector;
import org.apache.robux.msq.exec.ProcessingBuffersProvider;
import org.apache.robux.msq.exec.ProcessingBuffersSet;
import org.apache.robux.msq.exec.TaskDataSegmentProvider;
import org.apache.robux.msq.exec.Worker;
import org.apache.robux.msq.exec.WorkerClient;
import org.apache.robux.msq.exec.WorkerContext;
import org.apache.robux.msq.exec.WorkerMemoryParameters;
import org.apache.robux.msq.exec.WorkerStorageParameters;
import org.apache.robux.msq.guice.MultiStageQuery;
import org.apache.robux.msq.indexing.client.IndexerControllerClient;
import org.apache.robux.msq.indexing.client.IndexerWorkerClient;
import org.apache.robux.msq.indexing.client.WorkerChatHandler;
import org.apache.robux.msq.kernel.WorkOrder;
import org.apache.robux.msq.util.MultiStageQueryContext;
import org.apache.robux.query.QueryContext;
import org.apache.robux.query.QueryToolChestWarehouse;
import org.apache.robux.query.policy.PolicyEnforcer;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.rpc.ServiceLocator;
import org.apache.robux.rpc.StandardRetryPolicy;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.rpc.indexing.SpecificTaskRetryPolicy;
import org.apache.robux.rpc.indexing.SpecificTaskServiceLocator;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.loading.SegmentCacheManager;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.storage.StorageConnector;
import org.apache.robux.storage.StorageConnectorProvider;

import java.io.File;

public class IndexerWorkerContext implements WorkerContext
{
  private static final Logger log = new Logger(IndexerWorkerContext.class);

  private final MSQWorkerTask task;
  private final TaskToolbox toolbox;
  private final Injector injector;
  private final OverlordClient overlordClient;
  private final ServiceLocator controllerLocator;
  private final IndexIO indexIO;
  private final TaskDataSegmentProvider dataSegmentProvider;
  private final IndexerDataServerQueryHandlerFactory dataServerQueryHandlerFactory;
  private final ServiceClientFactory clientFactory;
  private final MemoryIntrospector memoryIntrospector;
  private final ProcessingBuffersProvider processingBuffersProvider;
  private final int maxConcurrentStages;
  private final boolean includeAllCounters;

  // Written under synchronized(this) using double-checked locking.
  private volatile ResourceHolder<ProcessingBuffersSet> processingBuffersSet;

  public IndexerWorkerContext(
      final MSQWorkerTask task,
      final TaskToolbox toolbox,
      final Injector injector,
      final OverlordClient overlordClient,
      final ServiceLocator controllerLocator,
      final IndexIO indexIO,
      final TaskDataSegmentProvider dataSegmentProvider,
      final ServiceClientFactory clientFactory,
      final MemoryIntrospector memoryIntrospector,
      final ProcessingBuffersProvider processingBuffersProvider,
      final IndexerDataServerQueryHandlerFactory dataServerQueryHandlerFactory
  )
  {
    this.task = task;
    this.toolbox = toolbox;
    this.overlordClient = overlordClient;
    this.controllerLocator = controllerLocator;
    this.indexIO = indexIO;
    this.dataSegmentProvider = dataSegmentProvider;
    this.clientFactory = clientFactory;
    this.memoryIntrospector = memoryIntrospector;
    this.processingBuffersProvider = processingBuffersProvider;
    this.dataServerQueryHandlerFactory = dataServerQueryHandlerFactory;

    final QueryContext queryContext = QueryContext.of(task.getContext());
    this.maxConcurrentStages = MultiStageQueryContext.getMaxConcurrentStagesWithDefault(
        queryContext,
        IndexerControllerContext.DEFAULT_MAX_CONCURRENT_STAGES
    );
    this.includeAllCounters = MultiStageQueryContext.getIncludeAllCounters(queryContext);
    final StorageConnectorProvider storageConnectorProvider = injector.getInstance(Key.get(
        StorageConnectorProvider.class,
        MultiStageQuery.class
    ));
    final StorageConnector storageConnector = storageConnectorProvider.createStorageConnector(toolbox.getIndexingTmpDir());
    this.injector = injector.createChildInjector(
        binder -> binder.bind(Key.get(StorageConnector.class, MultiStageQuery.class))
                        .toInstance(storageConnector));
  }

  public static IndexerWorkerContext createProductionInstance(
      final MSQWorkerTask task,
      final TaskToolbox toolbox,
      final Injector injector
  )
  {
    final IndexIO indexIO = injector.getInstance(IndexIO.class);
    final SegmentCacheManager segmentCacheManager =
        injector.getInstance(SegmentCacheManagerFactory.class)
                .manufacturate(new File(toolbox.getIndexingTmpDir(), "segment-fetch"));
    final ServiceClientFactory serviceClientFactory =
        injector.getInstance(Key.get(ServiceClientFactory.class, EscalatedGlobal.class));
    final MemoryIntrospector memoryIntrospector = injector.getInstance(MemoryIntrospector.class);
    final OverlordClient overlordClient =
        injector.getInstance(OverlordClient.class).withRetryPolicy(StandardRetryPolicy.unlimited());
    final ProcessingBuffersProvider processingBuffersProvider = injector.getInstance(ProcessingBuffersProvider.class);
    final ObjectMapper smileMapper = injector.getInstance(Key.get(ObjectMapper.class, Smile.class));
    final QueryToolChestWarehouse warehouse = injector.getInstance(QueryToolChestWarehouse.class);

    return new IndexerWorkerContext(
        task,
        toolbox,
        injector,
        overlordClient,
        new SpecificTaskServiceLocator(task.getControllerTaskId(), overlordClient),
        indexIO,
        new TaskDataSegmentProvider(toolbox.getCoordinatorClient(), segmentCacheManager, indexIO),
        serviceClientFactory,
        memoryIntrospector,
        processingBuffersProvider,
        new IndexerDataServerQueryHandlerFactory(
            toolbox.getCoordinatorClient(),
            serviceClientFactory,
            smileMapper,
            warehouse
        )
    );
  }

  @Override
  public String queryId()
  {
    return task.getControllerTaskId();
  }

  @Override
  public String workerId()
  {
    return task.getId();
  }

  public TaskToolbox toolbox()
  {
    return toolbox;
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return toolbox.getJsonMapper();
  }

  @Override
  public PolicyEnforcer policyEnforcer()
  {
    return toolbox.getPolicyEnforcer();
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public void emitMetric(MSQMetriceEventBuilder metricBuilder)
  {
    // Attach task specific dimensions
    metricBuilder.setTaskDimensions(task, QueryContext.of(task.getContext()));
    toolbox.getEmitter().emit(metricBuilder);
  }

  @Override
  public void registerWorker(Worker worker, Closer closer)
  {
    final WorkerChatHandler chatHandler =
        new WorkerChatHandler(worker, toolbox.getAuthorizerMapper(), task.getDataSource());
    toolbox.getChatHandlerProvider().register(worker.id(), chatHandler, false);
    closer.register(() -> toolbox.getChatHandlerProvider().unregister(worker.id()));
  }

  @Override
  public File tempDir()
  {
    return toolbox.getIndexingTmpDir();
  }

  @Override
  public int maxConcurrentStages()
  {
    return maxConcurrentStages;
  }

  @Override
  public ControllerClient makeControllerClient()
  {
    return new IndexerControllerClient(
        clientFactory.makeClient(
            task.getControllerTaskId(),
            controllerLocator,
            new SpecificTaskRetryPolicy(task.getControllerTaskId(), StandardRetryPolicy.unlimited())
        ),
        jsonMapper(),
        controllerLocator
    );
  }

  @Override
  public WorkerClient makeWorkerClient()
  {
    // Ignore workerId parameter. The workerId is passed into each method of WorkerClient individually.
    return new IndexerWorkerClient(clientFactory, overlordClient, jsonMapper());
  }

  @Override
  public FrameContext frameContext(WorkOrder workOrder)
  {
    if (processingBuffersSet == null) {
      synchronized (this) {
        if (processingBuffersSet == null) {
          processingBuffersSet = processingBuffersProvider.acquire(
              workOrder.getQueryDefinition(),
              maxConcurrentStages()
          );
        }
      }
    }

    final WorkerMemoryParameters memoryParameters =
        WorkerMemoryParameters.createProductionInstance(workOrder, memoryIntrospector, maxConcurrentStages);
    log.info("Memory parameters for stage[%s]: %s", workOrder.getStageDefinition().getId(), memoryParameters);

    return new IndexerFrameContext(
        workOrder.getStageDefinition().getId(),
        this,
        FrameWriterSpec.fromContext(workOrder.getWorkerContext()),
        indexIO,
        dataSegmentProvider,
        processingBuffersSet.get().acquireForStage(workOrder.getStageDefinition()),
        dataServerQueryHandlerFactory,
        memoryParameters,
        WorkerStorageParameters.createProductionInstance(injector, workOrder.getOutputChannelMode())
    );
  }

  @Override
  public int threadCount()
  {
    return memoryIntrospector.numProcessingThreads();
  }

  @Override
  public RobuxNode selfNode()
  {
    return toolbox.getRobuxNode();
  }

  @Override
  public DataServerQueryHandlerFactory dataServerQueryHandlerFactory()
  {
    return dataServerQueryHandlerFactory;
  }

  @Override
  public boolean includeAllCounters()
  {
    return includeAllCounters;
  }

  public ServiceLocator controllerLocator()
  {
    return controllerLocator;
  }

  @Override
  public void close()
  {
    controllerLocator.close();

    synchronized (this) {
      if (processingBuffersSet != null) {
        processingBuffersSet.close();
        processingBuffersSet = null;
      }
    }
  }
}

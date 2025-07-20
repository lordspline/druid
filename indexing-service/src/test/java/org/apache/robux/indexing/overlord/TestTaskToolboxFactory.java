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

package org.apache.robux.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;
import org.apache.robux.client.cache.Cache;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.client.cache.CachePopulatorStats;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.client.coordinator.NoopCoordinatorClient;
import org.apache.robux.discovery.DataNodeService;
import org.apache.robux.discovery.RobuxNodeAnnouncer;
import org.apache.robux.discovery.LookupNodeService;
import org.apache.robux.indexer.report.TaskReportFileWriter;
import org.apache.robux.indexing.common.SegmentCacheManagerFactory;
import org.apache.robux.indexing.common.TaskToolboxFactory;
import org.apache.robux.indexing.common.actions.TaskActionClientFactory;
import org.apache.robux.indexing.common.config.TaskConfig;
import org.apache.robux.indexing.common.config.TaskConfigBuilder;
import org.apache.robux.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.robux.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClientProvider;
import org.apache.robux.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.robux.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.java.util.metrics.MonitorScheduler;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.QueryProcessingPool;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.policy.PolicyEnforcer;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMergerV9Factory;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.robux.segment.incremental.RowIngestionMetersFactory;
import org.apache.robux.segment.join.JoinableFactory;
import org.apache.robux.segment.loading.DataSegmentArchiver;
import org.apache.robux.segment.loading.DataSegmentKiller;
import org.apache.robux.segment.loading.DataSegmentMover;
import org.apache.robux.segment.loading.DataSegmentPusher;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.segment.realtime.ChatHandlerProvider;
import org.apache.robux.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.robux.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordination.DataSegmentAnnouncer;
import org.apache.robux.server.coordination.DataSegmentServerAnnouncer;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.tasklogs.TaskLogPusher;
import org.apache.robux.utils.RuntimeInfo;

public class TestTaskToolboxFactory extends TaskToolboxFactory
{
  /**
   * We use a constructor that takes a builder instead of having the builder build the object so that
   * implementations can override methods on this class if they need to.
   *
   * @param bob the builder
   */
  public TestTaskToolboxFactory(
      Builder bob
  )
  {
    super(
        null,
        bob.config,
        bob.taskExecutorNode,
        bob.taskActionClientFactory,
        bob.emitter,
        bob.policyEnforcer,
        bob.segmentPusher,
        bob.dataSegmentKiller,
        bob.dataSegmentMover,
        bob.dataSegmentArchiver,
        bob.segmentAnnouncer,
        bob.serverAnnouncer,
        bob.handoffNotifierFactory,
        bob.queryRunnerFactoryConglomerateProvider,
        bob.processingConfigProvider,
        bob.queryProcessingPool,
        bob.joinableFactory,
        bob.monitorSchedulerProvider,
        bob.segmentCacheManagerFactory,
        bob.jsonMapper,
        bob.indexIO,
        bob.cache,
        bob.cacheConfig,
        bob.cachePopulatorStats,
        bob.indexMergerV9Factory,
        bob.robuxNodeAnnouncer,
        bob.robuxNode,
        bob.lookupNodeService,
        bob.dataNodeService,
        bob.taskReportFileWriter,
        bob.intermediaryDataManager,
        bob.authorizerMapper,
        bob.chatHandlerProvider,
        bob.rowIngestionMetersFactory,
        bob.appenderatorsManager,
        bob.overlordClient,
        bob.coordinatorClient,
        bob.supervisorTaskClientProvider,
        bob.shuffleClient,
        bob.taskLogPusher,
        bob.attemptId,
        bob.centralizedDatasourceSchemaConfig,
        bob.runtimeInfo
    );
  }

  public static class Builder
  {
    private TaskConfig config = new TaskConfigBuilder().build();
    private RobuxNode taskExecutorNode;
    private TaskActionClientFactory taskActionClientFactory = task -> null;
    private ServiceEmitter emitter;
    private PolicyEnforcer policyEnforcer;
    private DataSegmentPusher segmentPusher;
    private DataSegmentKiller dataSegmentKiller;
    private DataSegmentMover dataSegmentMover;
    private DataSegmentArchiver dataSegmentArchiver;
    private DataSegmentAnnouncer segmentAnnouncer;
    private DataSegmentServerAnnouncer serverAnnouncer;
    private SegmentHandoffNotifierFactory handoffNotifierFactory;
    private Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider;
    private Provider<RobuxProcessingConfig> processingConfigProvider;
    private QueryProcessingPool queryProcessingPool;
    private JoinableFactory joinableFactory;
    private Provider<MonitorScheduler> monitorSchedulerProvider;
    private ObjectMapper jsonMapper = TestHelper.JSON_MAPPER;
    private IndexIO indexIO = TestHelper.getTestIndexIO();
    private SegmentCacheManagerFactory segmentCacheManagerFactory = new SegmentCacheManagerFactory(TestIndex.INDEX_IO, jsonMapper);
    private Cache cache;
    private CacheConfig cacheConfig;
    private CachePopulatorStats cachePopulatorStats;
    private IndexMergerV9Factory indexMergerV9Factory = new IndexMergerV9Factory(jsonMapper, indexIO, OnHeapMemorySegmentWriteOutMediumFactory.instance());
    private RobuxNodeAnnouncer robuxNodeAnnouncer;
    private RobuxNode robuxNode;
    private LookupNodeService lookupNodeService;
    private DataNodeService dataNodeService;
    private TaskReportFileWriter taskReportFileWriter = new NoopTestTaskReportFileWriter();
    private IntermediaryDataManager intermediaryDataManager;
    private AuthorizerMapper authorizerMapper;
    private ChatHandlerProvider chatHandlerProvider;
    private RowIngestionMetersFactory rowIngestionMetersFactory;
    private AppenderatorsManager appenderatorsManager;
    private OverlordClient overlordClient = new NoopOverlordClient();
    private CoordinatorClient coordinatorClient = new NoopCoordinatorClient();
    private ParallelIndexSupervisorTaskClientProvider supervisorTaskClientProvider;
    private ShuffleClient shuffleClient;
    private TaskLogPusher taskLogPusher;
    private String attemptId;
    private CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;
    private RuntimeInfo runtimeInfo;

    public Builder setConfig(TaskConfig config)
    {
      this.config = config;
      return this;
    }

    public Builder setTaskExecutorNode(RobuxNode taskExecutorNode)
    {
      this.taskExecutorNode = taskExecutorNode;
      return this;
    }

    public Builder setTaskActionClientFactory(TaskActionClientFactory taskActionClientFactory)
    {
      this.taskActionClientFactory = taskActionClientFactory;
      return this;
    }

    public Builder setEmitter(ServiceEmitter emitter)
    {
      this.emitter = emitter;
      return this;
    }

    public Builder setPolicyEnforcer(PolicyEnforcer policyEnforcer)
    {
      this.policyEnforcer = policyEnforcer;
      return this;
    }

    public Builder setSegmentPusher(DataSegmentPusher segmentPusher)
    {
      this.segmentPusher = segmentPusher;
      return this;
    }

    public Builder setDataSegmentKiller(DataSegmentKiller dataSegmentKiller)
    {
      this.dataSegmentKiller = dataSegmentKiller;
      return this;
    }

    public Builder setDataSegmentMover(DataSegmentMover dataSegmentMover)
    {
      this.dataSegmentMover = dataSegmentMover;
      return this;
    }

    public Builder setDataSegmentArchiver(DataSegmentArchiver dataSegmentArchiver)
    {
      this.dataSegmentArchiver = dataSegmentArchiver;
      return this;
    }

    public Builder setSegmentAnnouncer(DataSegmentAnnouncer segmentAnnouncer)
    {
      this.segmentAnnouncer = segmentAnnouncer;
      return this;
    }

    public Builder setServerAnnouncer(DataSegmentServerAnnouncer serverAnnouncer)
    {
      this.serverAnnouncer = serverAnnouncer;
      return this;
    }

    public Builder setHandoffNotifierFactory(SegmentHandoffNotifierFactory handoffNotifierFactory)
    {
      this.handoffNotifierFactory = handoffNotifierFactory;
      return this;
    }

    public Builder setQueryRunnerFactoryConglomerateProvider(Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider)
    {
      this.queryRunnerFactoryConglomerateProvider = queryRunnerFactoryConglomerateProvider;
      return this;
    }

    public Builder setProcessingConfigProvider(Provider<RobuxProcessingConfig> processingConfigProvider)
    {
      this.processingConfigProvider = processingConfigProvider;
      return this;
    }

    public Builder setQueryProcessingPool(QueryProcessingPool queryProcessingPool)
    {
      this.queryProcessingPool = queryProcessingPool;
      return this;
    }

    public Builder setJoinableFactory(JoinableFactory joinableFactory)
    {
      this.joinableFactory = joinableFactory;
      return this;
    }

    public Builder setMonitorSchedulerProvider(Provider<MonitorScheduler> monitorSchedulerProvider)
    {
      this.monitorSchedulerProvider = monitorSchedulerProvider;
      return this;
    }

    public Builder setSegmentCacheManagerFactory(SegmentCacheManagerFactory segmentCacheManagerFactory)
    {
      this.segmentCacheManagerFactory = segmentCacheManagerFactory;
      return this;
    }

    public Builder setJsonMapper(ObjectMapper jsonMapper)
    {
      this.jsonMapper = jsonMapper;
      return this;
    }

    public Builder setIndexIO(IndexIO indexIO)
    {
      this.indexIO = indexIO;
      return this;
    }

    public Builder setCache(Cache cache)
    {
      this.cache = cache;
      return this;
    }

    public Builder setCacheConfig(CacheConfig cacheConfig)
    {
      this.cacheConfig = cacheConfig;
      return this;
    }

    public Builder setCachePopulatorStats(CachePopulatorStats cachePopulatorStats)
    {
      this.cachePopulatorStats = cachePopulatorStats;
      return this;
    }

    public Builder setIndexMergerV9Factory(IndexMergerV9Factory indexMergerV9Factory)
    {
      this.indexMergerV9Factory = indexMergerV9Factory;
      return this;
    }

    public Builder setRobuxNodeAnnouncer(RobuxNodeAnnouncer robuxNodeAnnouncer)
    {
      this.robuxNodeAnnouncer = robuxNodeAnnouncer;
      return this;
    }

    public Builder setRobuxNode(RobuxNode robuxNode)
    {
      this.robuxNode = robuxNode;
      return this;
    }

    public Builder setLookupNodeService(LookupNodeService lookupNodeService)
    {
      this.lookupNodeService = lookupNodeService;
      return this;
    }

    public Builder setDataNodeService(DataNodeService dataNodeService)
    {
      this.dataNodeService = dataNodeService;
      return this;
    }

    public Builder setTaskReportFileWriter(TaskReportFileWriter taskReportFileWriter)
    {
      this.taskReportFileWriter = taskReportFileWriter;
      return this;
    }

    public Builder setIntermediaryDataManager(IntermediaryDataManager intermediaryDataManager)
    {
      this.intermediaryDataManager = intermediaryDataManager;
      return this;
    }

    public Builder setAuthorizerMapper(AuthorizerMapper authorizerMapper)
    {
      this.authorizerMapper = authorizerMapper;
      return this;
    }

    public Builder setChatHandlerProvider(ChatHandlerProvider chatHandlerProvider)
    {
      this.chatHandlerProvider = chatHandlerProvider;
      return this;
    }

    public Builder setRowIngestionMetersFactory(RowIngestionMetersFactory rowIngestionMetersFactory)
    {
      this.rowIngestionMetersFactory = rowIngestionMetersFactory;
      return this;
    }

    public Builder setAppenderatorsManager(AppenderatorsManager appenderatorsManager)
    {
      this.appenderatorsManager = appenderatorsManager;
      return this;
    }

    public Builder setOverlordClient(OverlordClient overlordClient)
    {
      this.overlordClient = overlordClient;
      return this;
    }

    public Builder setCoordinatorClient(CoordinatorClient coordinatorClient)
    {
      this.coordinatorClient = coordinatorClient;
      return this;
    }

    public Builder setSupervisorTaskClientProvider(ParallelIndexSupervisorTaskClientProvider supervisorTaskClientProvider)
    {
      this.supervisorTaskClientProvider = supervisorTaskClientProvider;
      return this;
    }

    public Builder setShuffleClient(ShuffleClient shuffleClient)
    {
      this.shuffleClient = shuffleClient;
      return this;
    }

    public Builder setTaskLogPusher(TaskLogPusher taskLogPusher)
    {
      this.taskLogPusher = taskLogPusher;
      return this;
    }

    public Builder setAttemptId(String attemptId)
    {
      this.attemptId = attemptId;
      return this;
    }

    public Builder setCentralizedTableSchemaConfig(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
    {
      this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
      return this;
    }

    public Builder setRuntimeInfo(RuntimeInfo runtimeInfo)
    {
      this.runtimeInfo = runtimeInfo;
      return this;
    }
  }
}

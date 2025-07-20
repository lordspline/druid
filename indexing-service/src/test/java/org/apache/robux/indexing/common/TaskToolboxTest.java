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

package org.apache.robux.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.robux.client.cache.Cache;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.client.cache.CachePopulatorStats;
import org.apache.robux.client.coordinator.NoopCoordinatorClient;
import org.apache.robux.indexing.common.actions.TaskActionClientFactory;
import org.apache.robux.indexing.common.config.TaskConfig;
import org.apache.robux.indexing.common.config.TaskConfigBuilder;
import org.apache.robux.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.robux.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.robux.indexing.common.task.Task;
import org.apache.robux.indexing.common.task.Tasks;
import org.apache.robux.indexing.common.task.TestAppenderatorsManager;
import org.apache.robux.indexing.worker.config.WorkerConfig;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.java.util.metrics.MonitorScheduler;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.RobuxProcessingConfigTest;
import org.apache.robux.query.QueryProcessingPool;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMergerV9;
import org.apache.robux.segment.IndexMergerV9Factory;
import org.apache.robux.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.robux.segment.join.NoopJoinableFactory;
import org.apache.robux.segment.loading.DataSegmentArchiver;
import org.apache.robux.segment.loading.DataSegmentKiller;
import org.apache.robux.segment.loading.DataSegmentMover;
import org.apache.robux.segment.loading.DataSegmentPusher;
import org.apache.robux.segment.loading.SegmentLoaderConfig;
import org.apache.robux.segment.loading.SegmentLocalCacheManager;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.segment.realtime.NoopChatHandlerProvider;
import org.apache.robux.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.robux.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordination.DataSegmentAnnouncer;
import org.apache.robux.server.coordination.DataSegmentServerAnnouncer;
import org.apache.robux.server.security.AuthTestUtils;
import org.apache.robux.utils.JvmUtils;
import org.apache.robux.utils.RuntimeInfo;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;

@SuppressWarnings("DoNotMock")
public class TaskToolboxTest
{

  private TaskToolboxFactory taskToolbox = null;
  private TaskActionClientFactory mockTaskActionClientFactory = EasyMock.createMock(TaskActionClientFactory.class);
  private ServiceEmitter mockEmitter = EasyMock.createMock(ServiceEmitter.class);
  private DataSegmentPusher mockSegmentPusher = EasyMock.createMock(DataSegmentPusher.class);
  private DataSegmentKiller mockDataSegmentKiller = EasyMock.createMock(DataSegmentKiller.class);
  private DataSegmentMover mockDataSegmentMover = EasyMock.createMock(DataSegmentMover.class);
  private DataSegmentArchiver mockDataSegmentArchiver = EasyMock.createMock(DataSegmentArchiver.class);
  private DataSegmentAnnouncer mockSegmentAnnouncer = EasyMock.createMock(DataSegmentAnnouncer.class);
  private SegmentHandoffNotifierFactory mockHandoffNotifierFactory = EasyMock.createNiceMock(
      SegmentHandoffNotifierFactory.class
  );
  private QueryRunnerFactoryConglomerate mockQueryRunnerFactoryConglomerate
      = EasyMock.createMock(QueryRunnerFactoryConglomerate.class);
  private MonitorScheduler mockMonitorScheduler = EasyMock.createMock(MonitorScheduler.class);
  private QueryProcessingPool mockQueryProcessingPool = EasyMock.createMock(QueryProcessingPool.class);
  private ObjectMapper ObjectMapper = new ObjectMapper();
  private SegmentCacheManagerFactory mockSegmentCacheManagerFactory = EasyMock.createMock(SegmentCacheManagerFactory.class);
  private SegmentLocalCacheManager mockSegmentLoaderLocalCacheManager = EasyMock.createMock(SegmentLocalCacheManager.class);
  private Task task = EasyMock.createMock(Task.class);
  private IndexMergerV9Factory mockIndexMergerV9 = EasyMock.createMock(IndexMergerV9Factory.class);
  private IndexIO mockIndexIO = EasyMock.createMock(IndexIO.class);
  private Cache mockCache = EasyMock.createMock(Cache.class);
  private CacheConfig mockCacheConfig = EasyMock.createMock(CacheConfig.class);
  private SegmentLoaderConfig segmentLoaderConfig = EasyMock.createMock(SegmentLoaderConfig.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    EasyMock.expect(task.getId()).andReturn("task_id").anyTimes();
    EasyMock.expect(task.getDataSource()).andReturn("task_ds").anyTimes();
    EasyMock.expect(task.getContextValue(Tasks.STORE_EMPTY_COLUMNS_KEY, true)).andReturn(true).anyTimes();
    IndexMergerV9 indexMergerV9 = EasyMock.createMock(IndexMergerV9.class);
    EasyMock.expect(mockIndexMergerV9.create(true)).andReturn(indexMergerV9).anyTimes();
    EasyMock.replay(task, mockHandoffNotifierFactory, mockIndexMergerV9);

    TaskConfig taskConfig = new TaskConfigBuilder()
        .setBaseDir(temporaryFolder.newFile().toString())
        .build();

    taskToolbox = new TaskToolboxFactory(
        segmentLoaderConfig,
        taskConfig,
        new RobuxNode("robux/middlemanager", "localhost", false, 8091, null, true, false),
        mockTaskActionClientFactory,
        mockEmitter,
        NoopPolicyEnforcer.instance(),
        mockSegmentPusher,
        mockDataSegmentKiller,
        mockDataSegmentMover,
        mockDataSegmentArchiver,
        mockSegmentAnnouncer,
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        mockHandoffNotifierFactory,
        () -> mockQueryRunnerFactoryConglomerate,
        RobuxProcessingConfig::new,
        mockQueryProcessingPool,
        NoopJoinableFactory.INSTANCE,
        () -> mockMonitorScheduler,
        mockSegmentCacheManagerFactory,
        ObjectMapper,
        mockIndexIO,
        mockCache,
        mockCacheConfig,
        new CachePopulatorStats(),
        mockIndexMergerV9,
        null,
        null,
        null,
        null,
        new NoopTestTaskReportFileWriter(),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        new DropwizardRowIngestionMetersFactory(),
        new TestAppenderatorsManager(),
        new NoopOverlordClient(),
        new NoopCoordinatorClient(),
        null,
        null,
        null,
        "1",
        CentralizedDatasourceSchemaConfig.create(),
        JvmUtils.getRuntimeInfo()
    );
  }

  @Test
  public void testGetDataSegmentArchiver()
  {
    Assert.assertEquals(mockDataSegmentArchiver, taskToolbox.build(task).getDataSegmentArchiver());
  }

  @Test
  public void testGetSegmentLoaderConfig()
  {
    Assert.assertEquals(segmentLoaderConfig, taskToolbox.build(task).getSegmentLoaderConfig());
  }

  @Test
  public void testGetSegmentAnnouncer()
  {
    Assert.assertEquals(mockSegmentAnnouncer, taskToolbox.build(task).getSegmentAnnouncer());
  }

  @Test
  public void testGetQueryRunnerFactoryConglomerate()
  {
    Assert.assertEquals(
        mockQueryRunnerFactoryConglomerate,
        taskToolbox.build(task).getQueryRunnerFactoryConglomerate()
    );
  }

  @Test
  public void testGetQueryProcessingPool()
  {
    Assert.assertEquals(mockQueryProcessingPool, taskToolbox.build(task).getQueryProcessingPool());
  }

  @Test
  public void testGetMonitorScheduler()
  {
    Assert.assertEquals(mockMonitorScheduler, taskToolbox.build(task).getMonitorScheduler());
  }

  @Test
  public void testGetObjectMapper()
  {
    Assert.assertEquals(ObjectMapper, taskToolbox.build(task).getJsonMapper());
  }

  @Test
  public void testGetEmitter()
  {
    Assert.assertEquals(mockEmitter, taskToolbox.build(task).getEmitter());
  }

  @Test
  public void testGetDataSegmentKiller()
  {
    Assert.assertEquals(mockDataSegmentKiller, taskToolbox.build(task).getDataSegmentKiller());
  }

  @Test
  public void testGetDataSegmentMover()
  {
    Assert.assertEquals(mockDataSegmentMover, taskToolbox.build(task).getDataSegmentMover());
  }

  @Test
  public void testGetCache()
  {
    Assert.assertEquals(mockCache, taskToolbox.build(task).getCache());
  }

  @Test
  public void testGetCacheConfig()
  {
    Assert.assertEquals(mockCacheConfig, taskToolbox.build(task).getCacheConfig());
  }

  @Test
  public void testCreateAdjustedRuntimeInfoForMiddleManager()
  {
    final AppenderatorsManager appenderatorsManager = Mockito.mock(AppenderatorsManager.class);

    final RobuxProcessingConfigTest.MockRuntimeInfo runtimeInfo =
        new RobuxProcessingConfigTest.MockRuntimeInfo(12, 1_000_000, 2_000_000);
    final RuntimeInfo adjustedRuntimeInfo = TaskToolbox.createAdjustedRuntimeInfo(runtimeInfo, appenderatorsManager);

    Assert.assertEquals(
        runtimeInfo.getAvailableProcessors(),
        adjustedRuntimeInfo.getAvailableProcessors()
    );

    Assert.assertEquals(
        runtimeInfo.getMaxHeapSizeBytes(),
        adjustedRuntimeInfo.getMaxHeapSizeBytes()
    );

    Assert.assertEquals(
        runtimeInfo.getDirectMemorySizeBytes(),
        adjustedRuntimeInfo.getDirectMemorySizeBytes()
    );

    Mockito.verifyNoMoreInteractions(appenderatorsManager);
  }

  @Test
  public void testCreateAdjustedRuntimeInfoForIndexer()
  {
    // UnifiedIndexerAppenderatorsManager class is used on Indexers.
    final UnifiedIndexerAppenderatorsManager appenderatorsManager =
        Mockito.mock(UnifiedIndexerAppenderatorsManager.class);

    final int numWorkers = 3;
    final RobuxProcessingConfigTest.MockRuntimeInfo runtimeInfo =
        new RobuxProcessingConfigTest.MockRuntimeInfo(12, 1_000_000, 2_000_000);

    Mockito.when(appenderatorsManager.getWorkerConfig()).thenReturn(new WorkerConfig()
    {
      @Override
      public int getCapacity()
      {
        return 3;
      }
    });

    final RuntimeInfo adjustedRuntimeInfo = TaskToolbox.createAdjustedRuntimeInfo(runtimeInfo, appenderatorsManager);

    Assert.assertEquals(
        runtimeInfo.getAvailableProcessors() / numWorkers,
        adjustedRuntimeInfo.getAvailableProcessors()
    );

    Assert.assertEquals(
        runtimeInfo.getMaxHeapSizeBytes() / numWorkers,
        adjustedRuntimeInfo.getMaxHeapSizeBytes()
    );

    Assert.assertEquals(
        runtimeInfo.getDirectMemorySizeBytes() / numWorkers,
        adjustedRuntimeInfo.getDirectMemorySizeBytes()
    );

    Mockito.verify(appenderatorsManager).getWorkerConfig();
    Mockito.verifyNoMoreInteractions(appenderatorsManager);
  }
}

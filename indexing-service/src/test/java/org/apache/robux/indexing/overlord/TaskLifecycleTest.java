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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.client.cache.CachePopulatorStats;
import org.apache.robux.client.cache.MapCache;
import org.apache.robux.client.coordinator.NoopCoordinatorClient;
import org.apache.robux.data.input.AbstractInputSource;
import org.apache.robux.data.input.InputRow;
import org.apache.robux.data.input.InputRowListPlusRawValues;
import org.apache.robux.data.input.InputRowSchema;
import org.apache.robux.data.input.InputSourceReader;
import org.apache.robux.data.input.InputStats;
import org.apache.robux.data.input.MapBasedInputRow;
import org.apache.robux.data.input.impl.DimensionsSpec;
import org.apache.robux.data.input.impl.NoopInputFormat;
import org.apache.robux.data.input.impl.TimestampSpec;
import org.apache.robux.discovery.DataNodeService;
import org.apache.robux.discovery.RobuxNodeAnnouncer;
import org.apache.robux.discovery.LookupNodeService;
import org.apache.robux.indexer.TaskLocation;
import org.apache.robux.indexer.TaskState;
import org.apache.robux.indexer.TaskStatus;
import org.apache.robux.indexer.granularity.UniformGranularitySpec;
import org.apache.robux.indexing.common.SegmentCacheManagerFactory;
import org.apache.robux.indexing.common.TaskLock;
import org.apache.robux.indexing.common.TaskLockType;
import org.apache.robux.indexing.common.TaskToolbox;
import org.apache.robux.indexing.common.TaskToolboxFactory;
import org.apache.robux.indexing.common.TestUtils;
import org.apache.robux.indexing.common.actions.LocalTaskActionClientFactory;
import org.apache.robux.indexing.common.actions.LockListAction;
import org.apache.robux.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.robux.indexing.common.actions.TaskActionClient;
import org.apache.robux.indexing.common.actions.TaskActionClientFactory;
import org.apache.robux.indexing.common.actions.TaskActionToolbox;
import org.apache.robux.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.robux.indexing.common.config.TaskConfig;
import org.apache.robux.indexing.common.config.TaskConfigBuilder;
import org.apache.robux.indexing.common.config.TaskStorageConfig;
import org.apache.robux.indexing.common.task.AbstractFixedIntervalTask;
import org.apache.robux.indexing.common.task.IndexTask;
import org.apache.robux.indexing.common.task.IndexTask.IndexIOConfig;
import org.apache.robux.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.robux.indexing.common.task.KillUnusedSegmentsTask;
import org.apache.robux.indexing.common.task.NoopTask;
import org.apache.robux.indexing.common.task.NoopTaskContextEnricher;
import org.apache.robux.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.robux.indexing.common.task.Task;
import org.apache.robux.indexing.common.task.TaskResource;
import org.apache.robux.indexing.common.task.TestAppenderatorsManager;
import org.apache.robux.indexing.common.task.TuningConfigBuilder;
import org.apache.robux.indexing.overlord.config.DefaultTaskConfig;
import org.apache.robux.indexing.overlord.config.TaskLockConfig;
import org.apache.robux.indexing.overlord.config.TaskQueueConfig;
import org.apache.robux.indexing.overlord.supervisor.SupervisorManager;
import org.apache.robux.indexing.test.TestDataSegmentAnnouncer;
import org.apache.robux.indexing.test.TestIndexerMetadataStorageCoordinator;
import org.apache.robux.indexing.worker.config.WorkerConfig;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.CloseableIterators;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.FileUtils;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.java.util.common.RE;
import org.apache.robux.java.util.common.Stopwatch;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.java.util.common.guava.Comparators;
import org.apache.robux.java.util.common.parsers.CloseableIterator;
import org.apache.robux.java.util.emitter.EmittingLogger;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.java.util.metrics.MonitorScheduler;
import org.apache.robux.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.robux.metadata.TestDerbyConnector;
import org.apache.robux.query.DirectQueryProcessingPool;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.ForwardingQueryProcessingPool;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMergerV9Factory;
import org.apache.robux.segment.IndexSpec;
import org.apache.robux.segment.SegmentSchemaMapping;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.handoff.SegmentHandoffNotifier;
import org.apache.robux.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.robux.segment.indexing.DataSchema;
import org.apache.robux.segment.join.NoopJoinableFactory;
import org.apache.robux.segment.loading.DataSegmentPusher;
import org.apache.robux.segment.loading.LocalDataSegmentKiller;
import org.apache.robux.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.robux.segment.loading.NoopDataSegmentArchiver;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.segment.realtime.NoopChatHandlerProvider;
import org.apache.robux.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.robux.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordination.DataSegmentServerAnnouncer;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.server.security.AuthTestUtils;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.NoneShardSpec;
import org.apache.robux.utils.JvmUtils;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Hours;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

@RunWith(Parameterized.class)
public class TaskLifecycleTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper MAPPER;
  private static final IndexMergerV9Factory INDEX_MERGER_V9_FACTORY;
  private static final IndexIO INDEX_IO;
  private static final TestUtils TEST_UTILS;

  static {
    TEST_UTILS = new TestUtils();
    MAPPER = TEST_UTILS.getTestObjectMapper();
    INDEX_MERGER_V9_FACTORY = TEST_UTILS.getIndexMergerV9Factory();
    INDEX_IO = TEST_UTILS.getTestIndexIO();
  }

  private static final String HEAP_TASK_STORAGE = "HeapMemoryTaskStorage";
  private static final String METADATA_TASK_STORAGE = "MetadataTaskStorage";

  @Parameterized.Parameters(name = "taskStorageType={0}")
  public static Collection<String[]> constructFeed()
  {
    return Arrays.asList(new String[][]{{HEAP_TASK_STORAGE}, {METADATA_TASK_STORAGE}});
  }

  public TaskLifecycleTest(String taskStorageType)
  {
    this.taskStorageType = taskStorageType;
  }

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final Ordering<DataSegment> BY_INTERVAL_ORDERING = new Ordering<>()
  {
    @Override
    public int compare(DataSegment dataSegment, DataSegment dataSegment2)
    {
      return Comparators.intervalsByStartThenEnd().compare(dataSegment.getInterval(), dataSegment2.getInterval());
    }
  };
  private static DateTime now = DateTimes.nowUtc();

  private static final Iterable<InputRow> REALTIME_IDX_TASK_INPUT_ROWS = ImmutableList.of(
      ir(now.toString("YYYY-MM-dd'T'HH:mm:ss"), "test_dim1", "test_dim2", 1.0f),
      ir(now.plus(new Period(Hours.ONE)).toString("YYYY-MM-dd'T'HH:mm:ss"), "test_dim1", "test_dim2", 2.0f),
      ir(now.plus(new Period(Hours.TWO)).toString("YYYY-MM-dd'T'HH:mm:ss"), "test_dim1", "test_dim2", 3.0f)
  );

  private static final Iterable<InputRow> IDX_TASK_INPUT_ROWS = ImmutableList.of(
      ir("2010-01-01T01", "x", "y", 1),
      ir("2010-01-01T01", "x", "z", 1),
      ir("2010-01-02T01", "a", "b", 2),
      ir("2010-01-02T01", "a", "c", 1)
  );

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private final String taskStorageType;

  private ObjectMapper mapper;
  private TaskQueryTool tsqa = null;
  private TaskStorage taskStorage = null;
  private GlobalTaskLockbox taskLockbox = null;
  private TaskQueue taskQueue = null;
  private TaskRunner taskRunner = null;
  private TestIndexerMetadataStorageCoordinator mdc = null;
  private TaskActionClientFactory tac = null;
  private TaskToolboxFactory tb = null;
  private IndexSpec indexSpec;
  private QueryRunnerFactoryConglomerate queryRunnerFactoryConglomerate;
  private MonitorScheduler monitorScheduler;
  private ServiceEmitter emitter;
  private TaskLockConfig lockConfig;
  private TaskQueueConfig tqc;
  private TaskConfig taskConfig;
  private DataSegmentPusher dataSegmentPusher;
  private RobuxNode robuxNode = new RobuxNode("dummy", "dummy", false, 10000, null, true, false);
  private TaskLocation taskLocation = TaskLocation.create(
      robuxNode.getHost(),
      robuxNode.getPlaintextPort(),
      robuxNode.getTlsPort()
  );
  private int pushedSegments;
  private int announcedSinks;
  private SegmentHandoffNotifierFactory handoffNotifierFactory;
  private Map<SegmentDescriptor, Pair<Executor, Runnable>> handOffCallbacks;

  private static CountDownLatch publishCountDown;

  private static ServiceEmitter newMockEmitter()
  {
    return new NoopServiceEmitter();
  }

  private static InputRow ir(String dt, String dim1, String dim2, float met)
  {
    return new MapBasedInputRow(
        DateTimes.of(dt).getMillis(),
        ImmutableList.of("dim1", "dim2"),
        ImmutableMap.of(
            "dim1", dim1,
            "dim2", dim2,
            "met", met
        )
    );
  }

  private static class MockExceptionInputSource extends AbstractInputSource
  {
    @Override
    protected InputSourceReader fixedFormatReader(InputRowSchema inputRowSchema, @Nullable File temporaryDirectory)
    {
      return new InputSourceReader()
      {
        @Override
        public CloseableIterator<InputRow> read(InputStats inputStats)
        {
          return new CloseableIterator<>()
          {
            @Override
            public void close()
            {
            }

            @Override
            public boolean hasNext()
            {
              return true;
            }

            @Override
            public InputRow next()
            {
              throw new RuntimeException("HA HA HA");
            }
          };
        }

        @Override
        public CloseableIterator<InputRowListPlusRawValues> sample()
        {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public boolean needsFormat()
    {
      return false;
    }
  }

  private static class MockInputSource extends AbstractInputSource
  {
    @Override
    protected InputSourceReader fixedFormatReader(InputRowSchema inputRowSchema, @Nullable File temporaryDirectory)
    {
      return new InputSourceReader()
      {
        @Override
        public CloseableIterator<InputRow> read(InputStats inputStats)
        {
          final Iterator<InputRow> inputRowIterator = IDX_TASK_INPUT_ROWS.iterator();
          return CloseableIterators.withEmptyBaggage(inputRowIterator);
        }

        @Override
        public CloseableIterator<InputRowListPlusRawValues> sample()
        {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public boolean needsFormat()
    {
      return false;
    }
  }

  @Before
  public void setUp() throws Exception
  {
    // mock things
    queryRunnerFactoryConglomerate = EasyMock.createStrictMock(QueryRunnerFactoryConglomerate.class);
    monitorScheduler = EasyMock.createStrictMock(MonitorScheduler.class);

    // initialize variables
    announcedSinks = 0;
    pushedSegments = 0;
    indexSpec = IndexSpec.DEFAULT;
    emitter = newMockEmitter();
    EmittingLogger.registerEmitter(emitter);
    mapper = TEST_UTILS.getTestObjectMapper();
    handOffCallbacks = new ConcurrentHashMap<>();

    // Set up things, the order does matter as if it is messed up then the setUp
    // should fail because of the Precondition checks in the respective setUp methods
    // For creating a customized TaskQueue see testRealtimeIndexTaskFailure test

    taskStorage = setUpTaskStorage();
    handoffNotifierFactory = setUpSegmentHandOffNotifierFactory();
    dataSegmentPusher = setUpDataSegmentPusher();
    mdc = setUpMetadataStorageCoordinator();
    tb = setUpTaskToolboxFactory(dataSegmentPusher, handoffNotifierFactory, mdc);
    taskRunner = setUpThreadPoolTaskRunner(tb);
    taskQueue = setUpTaskQueue(taskStorage, taskRunner);
  }

  private TaskStorage setUpTaskStorage()
  {
    Preconditions.checkNotNull(mapper);
    Preconditions.checkNotNull(derbyConnectorRule);

    TaskStorage taskStorage;

    switch (taskStorageType) {
      case HEAP_TASK_STORAGE: {
        taskStorage = new HeapMemoryTaskStorage(
            new TaskStorageConfig(null)
        );
        break;
      }

      case METADATA_TASK_STORAGE: {
        TestDerbyConnector testDerbyConnector = derbyConnectorRule.getConnector();
        mapper.registerSubtypes(
            new NamedType(MockInputSource.class, "mockInputSource"),
            new NamedType(NoopInputFormat.class, "noopInputFormat")
        );
        testDerbyConnector.createTaskTables();
        testDerbyConnector.createSegmentSchemasTable();
        testDerbyConnector.createSegmentTable();
        taskStorage = new MetadataTaskStorage(
            testDerbyConnector,
            new TaskStorageConfig(null),
            new DerbyMetadataStorageActionHandlerFactory(
                testDerbyConnector,
                derbyConnectorRule.metadataTablesConfigSupplier().get(),
                mapper
            )
        );
        break;
      }

      default:
        throw new RE("Unknown task storage type [%s]", taskStorageType);
    }
    TaskMaster taskMaster = EasyMock.createMock(TaskMaster.class);
    EasyMock.expect(taskMaster.getTaskQueue()).andReturn(Optional.absent()).anyTimes();
    EasyMock.replay(taskMaster);
    tsqa = new TaskQueryTool(taskStorage, taskLockbox, taskMaster, null, null);
    return taskStorage;
  }

  private SegmentHandoffNotifierFactory setUpSegmentHandOffNotifierFactory()
  {
    Preconditions.checkNotNull(handOffCallbacks);

    return new SegmentHandoffNotifierFactory()
    {
      @Override
      public SegmentHandoffNotifier createSegmentHandoffNotifier(String dataSource, String taskId)
      {
        return new SegmentHandoffNotifier()
        {
          @Override
          public boolean registerSegmentHandoffCallback(
              SegmentDescriptor descriptor,
              Executor exec,
              Runnable handOffRunnable
          )
          {
            handOffCallbacks.put(descriptor, new Pair<>(exec, handOffRunnable));
            return true;
          }

          @Override
          public void start()
          {
            //Noop
          }

          @Override
          public void close()
          {
            //Noop
          }

        };
      }
    };
  }

  private DataSegmentPusher setUpDataSegmentPusher()
  {
    return new DataSegmentPusher()
    {
      @Override
      public String getPathForHadoop()
      {
        throw new UnsupportedOperationException();
      }

      @Deprecated
      @Override
      public String getPathForHadoop(String dataSource)
      {
        return getPathForHadoop();
      }

      @Override
      public DataSegment push(File file, DataSegment segment, boolean useUniquePath)
      {
        pushedSegments++;
        return segment;
      }

      @Override
      public Map<String, Object> makeLoadSpec(URI uri)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  private TestIndexerMetadataStorageCoordinator setUpMetadataStorageCoordinator()
  {
    return new TestIndexerMetadataStorageCoordinator()
    {
      @Override
      public Set<DataSegment> commitSegments(Set<DataSegment> segments, final SegmentSchemaMapping segmentSchemaMapping)
      {
        Set<DataSegment> retVal = super.commitSegments(segments, segmentSchemaMapping);
        if (publishCountDown != null) {
          publishCountDown.countDown();
        }
        return retVal;
      }
    };
  }

  private TaskToolboxFactory setUpTaskToolboxFactory(
      DataSegmentPusher dataSegmentPusher,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      TestIndexerMetadataStorageCoordinator mdc
  ) throws IOException
  {
    return setUpTaskToolboxFactory(dataSegmentPusher, handoffNotifierFactory, mdc, new TestAppenderatorsManager());
  }

  private TaskToolboxFactory setUpTaskToolboxFactory(
      DataSegmentPusher dataSegmentPusher,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      TestIndexerMetadataStorageCoordinator mdc,
      AppenderatorsManager appenderatorsManager
  ) throws IOException
  {
    Preconditions.checkNotNull(queryRunnerFactoryConglomerate);
    Preconditions.checkNotNull(monitorScheduler);
    Preconditions.checkNotNull(taskStorage);
    Preconditions.checkNotNull(emitter);

    taskLockbox = new GlobalTaskLockbox(taskStorage, mdc);
    tac = new LocalTaskActionClientFactory(
        new TaskActionToolbox(
            taskLockbox,
            taskStorage,
            mdc,
            emitter,
            EasyMock.createMock(SupervisorManager.class),
            mapper
        )
    );
    taskConfig = new TaskConfigBuilder()
        .setBaseDir(temporaryFolder.newFolder().toString())
        .setTmpStorageBytesPerTask(-1L)
        .build();

    return new TaskToolboxFactory(
        null,
        taskConfig,
        new RobuxNode("robux/middlemanager", "localhost", false, 8091, null, true, false),
        tac,
        emitter,
        NoopPolicyEnforcer.instance(),
        dataSegmentPusher,
        new LocalDataSegmentKiller(new LocalDataSegmentPusherConfig()),
        (dataSegment, targetLoadSpec) -> dataSegment,
        new NoopDataSegmentArchiver(),
        new TestDataSegmentAnnouncer()
        {
          @Override
          public void announceSegment(DataSegment segment)
          {
            announcedSinks++;
          }

        },
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        handoffNotifierFactory,
        () -> queryRunnerFactoryConglomerate, // query runner factory conglomerate corporation unionized collective
        RobuxProcessingConfig::new,
        DirectQueryProcessingPool.INSTANCE, // query executor service
        NoopJoinableFactory.INSTANCE,
        () -> monitorScheduler, // monitor scheduler
        new SegmentCacheManagerFactory(TestIndex.INDEX_IO, new DefaultObjectMapper()),
        MAPPER,
        INDEX_IO,
        MapCache.create(0),
        new CacheConfig(),
        new CachePopulatorStats(),
        INDEX_MERGER_V9_FACTORY,
        EasyMock.createNiceMock(RobuxNodeAnnouncer.class),
        EasyMock.createNiceMock(RobuxNode.class),
        new LookupNodeService("tier"),
        new DataNodeService("tier", 1000, ServerType.INDEXER_EXECUTOR, 0),
        new NoopTestTaskReportFileWriter(),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        TEST_UTILS.getRowIngestionMetersFactory(),
        appenderatorsManager,
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

  private TaskRunner setUpThreadPoolTaskRunner(TaskToolboxFactory tb)
  {
    Preconditions.checkNotNull(taskConfig);
    Preconditions.checkNotNull(emitter);

    return new SingleTaskBackgroundRunner(
        tb,
        taskConfig,
        emitter,
        robuxNode,
        new ServerConfig()
    );
  }

  private TaskQueue setUpTaskQueue(TaskStorage ts, TaskRunner tr) throws Exception
  {
    Preconditions.checkNotNull(taskLockbox);
    Preconditions.checkNotNull(tac);
    Preconditions.checkNotNull(emitter);

    lockConfig = new TaskLockConfig();
    tqc = mapper.readValue(
        "{\"startDelay\":\"PT0S\", \"restartDelay\":\"PT1S\", \"storageSyncRate\":\"PT0.5S\"}",
        TaskQueueConfig.class
    );

    return new TaskQueue(
        lockConfig,
        tqc,
        new DefaultTaskConfig(),
        ts,
        tr,
        tac,
        taskLockbox,
        emitter,
        mapper,
        new NoopTaskContextEnricher()
    );
  }

  @After
  public void tearDown()
  {
    if (taskQueue.isActive()) {
      taskQueue.stop();
    }
  }

  @Test
  public void testIndexTask()
  {
    final Task indexTask = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withTimestamp(new TimestampSpec(null, null, null))
                      .withDimensions(DimensionsSpec.EMPTY)
                      .withAggregators(new DoubleSumAggregatorFactory("met", "met"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                          )
                      )
                      .build(),
            new IndexIOConfig(new MockInputSource(), new NoopInputFormat(), false, false),
            TuningConfigBuilder.forIndexTask()
                               .withMaxRowsPerSegment(10000)
                               .withMaxRowsInMemory(100)
                               .withIndexSpec(indexSpec)
                               .withMaxPendingPersists(3)
                               .withForceGuaranteedRollup(false)
                               .build()
        ),
        null
    );

    final Optional<TaskStatus> preRunTaskStatus = tsqa.getTaskStatus(indexTask.getId());
    Assert.assertTrue("pre run task status not present", !preRunTaskStatus.isPresent());

    final TaskStatus mergedStatus = runTask(indexTask);
    final TaskStatus status = taskStorage.getStatus(indexTask.getId()).get();
    final List<DataSegment> publishedSegments = BY_INTERVAL_ORDERING.sortedCopy(mdc.getPublished());

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("merged statusCode", TaskState.SUCCESS, mergedStatus.getStatusCode());
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());

    Assert.assertEquals("segment1 datasource", "foo", publishedSegments.get(0).getDataSource());
    Assert.assertEquals("segment1 interval", Intervals.of("2010-01-01/P1D"), publishedSegments.get(0).getInterval());
    Assert.assertEquals(
        "segment1 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(0).getDimensions()
    );
    Assert.assertEquals("segment1 metrics", ImmutableList.of("met"), publishedSegments.get(0).getMetrics());

    Assert.assertEquals("segment2 datasource", "foo", publishedSegments.get(1).getDataSource());
    Assert.assertEquals("segment2 interval", Intervals.of("2010-01-02/P1D"), publishedSegments.get(1).getInterval());
    Assert.assertEquals(
        "segment2 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(1).getDimensions()
    );
    Assert.assertEquals("segment2 metrics", ImmutableList.of("met"), publishedSegments.get(1).getMetrics());
  }

  @Test
  public void testIndexTaskFailure()
  {
    final Task indexTask = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withAggregators(new DoubleSumAggregatorFactory("met", "met"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P1D"))
                          )
                      )
                      .withObjectMapper(mapper)
                      .build(),
            new IndexIOConfig(new MockExceptionInputSource(), new NoopInputFormat(), false, false),
            TuningConfigBuilder.forIndexTask()
                               .withMaxRowsPerSegment(10000)
                               .withMaxRowsInMemory(10)
                               .withIndexSpec(indexSpec)
                               .withMaxPendingPersists(3)
                               .withForceGuaranteedRollup(false)
                               .build()
        ),
        null
    );

    final TaskStatus status = runTask(indexTask);

    Assert.assertEquals("statusCode", TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testKillUnusedSegmentsTask() throws Exception
  {
    final File tmpSegmentDir = temporaryFolder.newFolder();

    List<DataSegment> expectedUnusedSegments = Lists.transform(
        ImmutableList.of(
            "2011-04-01/2011-04-02",
            "2011-04-02/2011-04-03",
            "2011-04-04/2011-04-05"
        ), new Function<>()
        {
          @Override
          public DataSegment apply(String input)
          {
            final Interval interval = Intervals.of(input);
            try {
              return DataSegment.builder()
                                .dataSource("test_kill_task")
                                .interval(interval)
                                .loadSpec(
                                    ImmutableMap.of(
                                        "type",
                                        "local",
                                        "path",
                                        tmpSegmentDir.getCanonicalPath()
                                        + "/robux/localStorage/wikipedia/"
                                        + interval.getStart()
                                        + "-"
                                        + interval.getEnd()
                                        + "/"
                                        + "2011-04-6T16:52:46.119-05:00"
                                        + "/0/index.zip"
                                    )
                                )
                                .version("2011-04-6T16:52:46.119-05:00")
                                .dimensions(ImmutableList.of())
                                .metrics(ImmutableList.of())
                                .shardSpec(NoneShardSpec.instance())
                                .binaryVersion(9)
                                .size(0)
                                .build();
            }
            catch (IOException e) {
              throw new ISE(e, "Error creating segments");
            }
          }
        }
    );

    mdc.commitSegments(Set.copyOf(expectedUnusedSegments), null);
    expectedUnusedSegments.forEach(segment -> mdc.markSegmentAsUnused(segment.getId()));

    // manually create local segments files
    List<File> segmentFiles = new ArrayList<>();
    final List<DataSegment> unusedSegments = mdc.retrieveUnusedSegmentsForInterval(
        "test_kill_task",
        Intervals.of("2011-04-01/P4D"),
        null,
        null
    );
    for (DataSegment segment : unusedSegments) {
      File file = new File((String) segment.getLoadSpec().get("path"));
      FileUtils.mkdirp(file.getParentFile());
      Files.write(file.toPath(), ByteArrays.EMPTY_ARRAY);
      segmentFiles.add(file);
    }

    final Task killUnusedSegmentsTask =
        new KillUnusedSegmentsTask(
            null,
            "test_kill_task",
            Intervals.of("2011-04-01/P4D"),
            null,
            null,
            null,
            null,
            null
        );

    final TaskStatus status = runTask(killUnusedSegmentsTask);
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("merged statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals("num segments published", 3, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 3, mdc.getNuked().size());
    Assert.assertEquals("delete segment batch call count", 2, mdc.getDeleteSegmentsCount());
    Assert.assertTrue(
        "expected unused segments get killed",
        expectedUnusedSegments.containsAll(mdc.getNuked()) && mdc.getNuked().containsAll(
            expectedUnusedSegments
        )
    );

    for (File file : segmentFiles) {
      Assert.assertFalse("unused segments files get deleted", file.exists());
    }
  }

  @Test
  public void testKillUnusedSegmentsTaskWithMaxSegmentsToKill() throws Exception
  {
    final File tmpSegmentDir = temporaryFolder.newFolder();

    List<DataSegment> expectedUnusedSegments = Lists.transform(
        ImmutableList.of(
            "2011-04-01/2011-04-02",
            "2011-04-02/2011-04-03",
            "2011-04-04/2011-04-05"
        ), new Function<>()
        {
          @Override
          public DataSegment apply(String input)
          {
            final Interval interval = Intervals.of(input);
            try {
              return DataSegment.builder()
                  .dataSource("test_kill_task")
                  .interval(interval)
                  .loadSpec(
                      ImmutableMap.of(
                          "type",
                          "local",
                          "path",
                          tmpSegmentDir.getCanonicalPath()
                          + "/robux/localStorage/wikipedia/"
                          + interval.getStart()
                          + "-"
                          + interval.getEnd()
                          + "/"
                          + "2011-04-6T16:52:46.119-05:00"
                          + "/0/index.zip"
                      )
                  )
                  .version("2011-04-6T16:52:46.119-05:00")
                  .dimensions(ImmutableList.of())
                  .metrics(ImmutableList.of())
                  .shardSpec(NoneShardSpec.instance())
                  .binaryVersion(9)
                  .size(0)
                  .build();
            }
            catch (IOException e) {
              throw new ISE(e, "Error creating segments");
            }
          }
        }
    );

    mdc.commitSegments(Set.copyOf(expectedUnusedSegments), null);
    expectedUnusedSegments.forEach(segment -> mdc.markSegmentAsUnused(segment.getId()));

    // manually create local segments files
    List<File> segmentFiles = new ArrayList<>();
    final List<DataSegment> unusedSegments = mdc.retrieveUnusedSegmentsForInterval(
        "test_kill_task",
        Intervals.of("2011-04-01/P4D"),
        null,
        null
    );
    for (DataSegment segment : unusedSegments) {
      File file = new File((String) segment.getLoadSpec().get("path"));
      FileUtils.mkdirp(file.getParentFile());
      Files.write(file.toPath(), ByteArrays.EMPTY_ARRAY);
      segmentFiles.add(file);
    }

    final int maxSegmentsToKill = 2;
    final Task killUnusedSegmentsTask =
        new KillUnusedSegmentsTask(
            null,
            "test_kill_task",
            Intervals.of("2011-04-01/P4D"),
            null,
            null,
            null,
            maxSegmentsToKill,
            null
        );

    final TaskStatus status = runTask(killUnusedSegmentsTask);
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("merged statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals("num segments published", 3, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", maxSegmentsToKill, mdc.getNuked().size());
    Assert.assertTrue(
        "expected unused segments get killed",
        expectedUnusedSegments.containsAll(mdc.getNuked())
    );

    int expectedNumOfSegmentsRemaining = segmentFiles.size() - maxSegmentsToKill;
    int actualNumOfSegmentsRemaining = 0;
    for (File file : segmentFiles) {
      if (file.exists()) {
        actualNumOfSegmentsRemaining++;
      }
    }

    Assert.assertEquals(
        "Expected of segments deleted did not match expectations",
        expectedNumOfSegmentsRemaining,
        actualNumOfSegmentsRemaining
    );
  }

  @Test
  public void testRealtimeishTask()
  {
    final Task rtishTask = new RealtimeishTask();
    final TaskStatus status = runTask(rtishTask);

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testNoopTask()
  {
    final TaskStatus status = runTask(NoopTask.create());

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testNeverReadyTask()
  {
    final Task neverReadyTask = new NoopTask(null, null, null, 0, 0, null)
    {
      @Override
      public boolean isReady(TaskActionClient taskActionClient)
      {
        throw new ISE("Task will never be ready");
      }
    };
    final TaskStatus status = runTask(neverReadyTask);

    Assert.assertEquals("statusCode", TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("num segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testSimple()
  {
    final Task task = new AbstractFixedIntervalTask(
        "id1",
        "id1",
        new TaskResource("id1", 1),
        "ds",
        Intervals.of("2012-01-01/P1D"),
        null
    )
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public void stopGracefully(TaskConfig taskConfig)
      {
      }

      @Override
      public TaskStatus runTask(TaskToolbox toolbox) throws Exception
      {
        final Interval interval = Intervals.of("2012-01-01/P1D");
        final TimeChunkLockTryAcquireAction action = new TimeChunkLockTryAcquireAction(
            TaskLockType.EXCLUSIVE,
            interval
        );

        final TaskLock lock = toolbox.getTaskActionClient().submit(action);
        if (lock == null) {
          throw new ISE("Failed to get a lock");
        }

        final DataSegment segment = DataSegment
            .builder()
            .dataSource("ds")
            .interval(interval)
            .version(lock.getVersion())
            .size(0)
            .build();

        toolbox.getTaskActionClient().submit(
            SegmentTransactionalInsertAction.appendAction(ImmutableSet.of(segment), null, null, null, null, null)
        );
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals("segments published", 1, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testBadInterval()
  {
    final Task task = new AbstractFixedIntervalTask("id1", "id1", "ds", Intervals.of("2012-01-01/P1D"), null)
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public void stopGracefully(TaskConfig taskConfig)
      {
      }

      @Override
      public TaskStatus runTask(TaskToolbox toolbox) throws Exception
      {
        final TaskLock myLock = Iterables.getOnlyElement(toolbox.getTaskActionClient().submit(new LockListAction()));

        final DataSegment segment = DataSegment
            .builder()
            .dataSource("ds")
            .interval(Intervals.of("2012-01-01/P2D"))
            .version(myLock.getVersion())
            .size(0)
            .build();

        toolbox.getTaskActionClient().submit(
            SegmentTransactionalInsertAction.appendAction(ImmutableSet.of(segment), null, null, null, null, null)
        );
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testBadVersion()
  {
    final Task task = new AbstractFixedIntervalTask("id1", "id1", "ds", Intervals.of("2012-01-01/P1D"), null)
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public void stopGracefully(TaskConfig taskConfig)
      {
      }

      @Override
      public TaskStatus runTask(TaskToolbox toolbox) throws Exception
      {
        final TaskLock myLock = Iterables.getOnlyElement(toolbox.getTaskActionClient().submit(new LockListAction()));

        final DataSegment segment = DataSegment
            .builder()
            .dataSource("ds")
            .interval(Intervals.of("2012-01-01/P1D"))
            .version(myLock.getVersion() + "1!!!1!!")
            .size(0)
            .build();

        toolbox.getTaskActionClient().submit(
            SegmentTransactionalInsertAction.appendAction(ImmutableSet.of(segment), null, null, null, null, null)
        );
        return TaskStatus.success(getId());
      }
    };

    final TaskStatus status = runTask(task);

    Assert.assertEquals("statusCode", TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  @Test
  public void testResumeTasks() throws Exception
  {
    final Task indexTask = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withTimestamp(new TimestampSpec(null, null, null))
                      .withDimensions(DimensionsSpec.EMPTY)
                      .withAggregators(new DoubleSumAggregatorFactory("met", "met"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                          )
                      )
                      .build(),
            new IndexIOConfig(new MockInputSource(), new NoopInputFormat(), false, false),
            TuningConfigBuilder.forIndexTask()
                               .withMaxRowsPerSegment(10000)
                               .withMaxRowsInMemory(10)
                               .withIndexSpec(indexSpec)
                               .build()
        ),
        null
    );

    final long startTime = System.currentTimeMillis();

    // manually insert the task into TaskStorage, waiting for TaskQueue to sync from storage
    taskQueue.start();
    taskStorage.insert(indexTask, TaskStatus.running(indexTask.getId()));

    while (tsqa.getTaskStatus(indexTask.getId()).get().isRunnable()) {
      if (System.currentTimeMillis() > startTime + 10 * 1000) {
        throw new ISE("Where did the task go?!: %s", indexTask.getId());
      }

      Thread.sleep(100);
    }

    final TaskStatus status = taskStorage.getStatus(indexTask.getId()).get();
    final List<DataSegment> publishedSegments = BY_INTERVAL_ORDERING.sortedCopy(mdc.getPublished());

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("num segments published", 2, mdc.getPublished().size());
    Assert.assertEquals("num segments nuked", 0, mdc.getNuked().size());

    Assert.assertEquals("segment1 datasource", "foo", publishedSegments.get(0).getDataSource());
    Assert.assertEquals("segment1 interval", Intervals.of("2010-01-01/P1D"), publishedSegments.get(0).getInterval());
    Assert.assertEquals(
        "segment1 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(0).getDimensions()
    );
    Assert.assertEquals("segment1 metrics", ImmutableList.of("met"), publishedSegments.get(0).getMetrics());

    Assert.assertEquals("segment2 datasource", "foo", publishedSegments.get(1).getDataSource());
    Assert.assertEquals("segment2 interval", Intervals.of("2010-01-02/P1D"), publishedSegments.get(1).getInterval());
    Assert.assertEquals(
        "segment2 dimensions",
        ImmutableList.of("dim1", "dim2"),
        publishedSegments.get(1).getDimensions()
    );
    Assert.assertEquals("segment2 metrics", ImmutableList.of("met"), publishedSegments.get(1).getMetrics());
  }

  @Test
  public void testUnifiedAppenderatorsManagerCleanup() throws Exception
  {
    final ExecutorService exec = Execs.multiThreaded(8, "TaskLifecycleTest-%d");

    UnifiedIndexerAppenderatorsManager unifiedIndexerAppenderatorsManager = new UnifiedIndexerAppenderatorsManager(
        new ForwardingQueryProcessingPool(exec),
        new WorkerConfig(),
        MapCache.create(2048),
        new CacheConfig(),
        new CachePopulatorStats(),
        NoopPolicyEnforcer.instance(),
        MAPPER,
        new NoopServiceEmitter(),
        () -> queryRunnerFactoryConglomerate
    );

    tb = setUpTaskToolboxFactory(dataSegmentPusher, handoffNotifierFactory, mdc, unifiedIndexerAppenderatorsManager);
    taskRunner = setUpThreadPoolTaskRunner(tb);
    taskQueue = setUpTaskQueue(taskStorage, taskRunner);

    final Task indexTask = new IndexTask(
        null,
        null,
        new IndexIngestionSpec(
            DataSchema.builder()
                      .withDataSource("foo")
                      .withTimestamp(new TimestampSpec(null, null, null))
                      .withDimensions(DimensionsSpec.EMPTY)
                      .withAggregators(new DoubleSumAggregatorFactory("met", "met"))
                      .withGranularity(
                          new UniformGranularitySpec(
                              Granularities.DAY,
                              null,
                              ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                          )
                      )
                      .build(),
            new IndexIOConfig(new MockInputSource(), new NoopInputFormat(), false, false),
            TuningConfigBuilder.forIndexTask()
                               .withMaxRowsPerSegment(10000)
                               .withMaxRowsInMemory(10)
                               .withIndexSpec(indexSpec)
                               .withMaxPendingPersists(3)
                               .withForceGuaranteedRollup(false)
                               .build()
        ),
        null
    );

    final Optional<TaskStatus> preRunTaskStatus = tsqa.getTaskStatus(indexTask.getId());
    Assert.assertTrue("pre run task status not present", !preRunTaskStatus.isPresent());

    final TaskStatus mergedStatus = runTask(indexTask);
    final TaskStatus status = taskStorage.getStatus(indexTask.getId()).get();

    Assert.assertEquals("statusCode", TaskState.SUCCESS, status.getStatusCode());

    Map<String, UnifiedIndexerAppenderatorsManager.DatasourceBundle> bundleMap =
        unifiedIndexerAppenderatorsManager.getDatasourceBundles();

    Assert.assertEquals(1, bundleMap.size());

    unifiedIndexerAppenderatorsManager.removeAppenderatorsForTask(indexTask.getId(), "foo");

    Assert.assertTrue(bundleMap.isEmpty());

  }

  @Test
  public void testLockRevoked()
  {
    final Task task = new AbstractFixedIntervalTask(
        "id1",
        "id1",
        new TaskResource("id1", 1),
        "ds",
        Intervals.of("2012-01-01/P1D"),
        null
    )
    {
      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public void stopGracefully(TaskConfig taskConfig)
      {
      }

      @Override
      public TaskStatus runTask(TaskToolbox toolbox) throws Exception
      {
        final Interval interval = Intervals.of("2012-01-01/P1D");
        final TimeChunkLockTryAcquireAction action = new TimeChunkLockTryAcquireAction(
            TaskLockType.EXCLUSIVE,
            interval
        );

        final TaskLock lock = toolbox.getTaskActionClient().submit(action);
        if (lock == null) {
          throw new ISE("Failed to get a lock");
        }

        final TaskLock lockBeforeRevoke = toolbox.getTaskActionClient().submit(action);
        Assert.assertFalse(lockBeforeRevoke.isRevoked());

        taskLockbox.revokeLock(getId(), lock);

        final TaskLock lockAfterRevoke = toolbox.getTaskActionClient().submit(action);
        Assert.assertTrue(lockAfterRevoke.isRevoked());
        return TaskStatus.failure(getId(), "lock revoked test");
      }
    };

    final TaskStatus status = runTask(task);
    Assert.assertEquals(taskLocation, status.getLocation());
    Assert.assertEquals("statusCode", TaskState.FAILED, status.getStatusCode());
    Assert.assertEquals("segments published", 0, mdc.getPublished().size());
    Assert.assertEquals("segments nuked", 0, mdc.getNuked().size());
  }

  private TaskStatus runTask(final Task task)
  {
    final Stopwatch taskRunDuration = Stopwatch.createStarted();

    // Since multiple tasks can be run in a single unit test using runTask(), hence this check and synchronization
    synchronized (this) {
      if (!taskQueue.isActive()) {
        taskQueue.start();
      }
    }
    taskQueue.add(task);

    final String taskId = task.getId();
    TaskStatus retVal = null;
    try {
      TaskStatus status;
      while ((status = tsqa.getTaskStatus(taskId).get()).isRunnable()) {
        if (taskRunDuration.millisElapsed() > 10_000) {
          throw new ISE("Where did the task go?!: %s", task.getId());
        }

        Thread.sleep(100);
      }
      if (taskId.equals(task.getId())) {
        retVal = status;
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    return retVal;
  }
}

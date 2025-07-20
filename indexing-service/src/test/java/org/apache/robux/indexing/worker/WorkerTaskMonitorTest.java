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

package org.apache.robux.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Joiner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.robux.client.coordinator.NoopCoordinatorClient;
import org.apache.robux.curator.PotentiallyGzippedCompressionProvider;
import org.apache.robux.curator.announcement.NodeAnnouncer;
import org.apache.robux.indexer.TaskState;
import org.apache.robux.indexing.common.IndexingServiceCondition;
import org.apache.robux.indexing.common.SegmentCacheManagerFactory;
import org.apache.robux.indexing.common.TaskToolboxFactory;
import org.apache.robux.indexing.common.TestIndexTask;
import org.apache.robux.indexing.common.TestTasks;
import org.apache.robux.indexing.common.TestUtils;
import org.apache.robux.indexing.common.actions.TaskActionClient;
import org.apache.robux.indexing.common.actions.TaskActionClientFactory;
import org.apache.robux.indexing.common.config.TaskConfig;
import org.apache.robux.indexing.common.config.TaskConfigBuilder;
import org.apache.robux.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.robux.indexing.common.task.Task;
import org.apache.robux.indexing.common.task.TestAppenderatorsManager;
import org.apache.robux.indexing.overlord.SingleTaskBackgroundRunner;
import org.apache.robux.indexing.overlord.TestRemoteTaskRunnerConfig;
import org.apache.robux.indexing.worker.config.WorkerConfig;
import org.apache.robux.java.util.common.FileUtils;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMergerV9Factory;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.robux.segment.join.NoopJoinableFactory;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.segment.realtime.NoopChatHandlerProvider;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.initialization.IndexerZkConfig;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.server.initialization.ZkPathsConfig;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.server.security.AuthTestUtils;
import org.apache.robux.utils.JvmUtils;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class WorkerTaskMonitorTest
{
  private static final Joiner JOINER = Joiner.on("/");
  private static final String BASE_PATH = "/test/robux";
  private static final String TASKS_PATH = StringUtils.format("%s/indexer/tasks/worker", BASE_PATH);
  private static final String STATUS_PATH = StringUtils.format("%s/indexer/status/worker", BASE_PATH);
  private static final RobuxNode DUMMY_NODE = new RobuxNode("dummy", "dummy", false, 9000, null, true, false);

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private WorkerCuratorCoordinator workerCuratorCoordinator;
  private WorkerTaskMonitor workerTaskMonitor;

  private Task task;

  private Worker worker;
  private final TestUtils testUtils;
  private ObjectMapper jsonMapper;
  private IndexMergerV9Factory indexMergerV9Factory;
  private IndexIO indexIO;

  public WorkerTaskMonitorTest()
  {
    testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    indexMergerV9Factory = testUtils.getIndexMergerV9Factory();
    indexIO = testUtils.getTestIndexIO();
  }

  @Before
  public void setUp() throws Exception
  {
    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
                                .build();
    cf.start();
    cf.blockUntilConnected();
    cf.create().creatingParentsIfNeeded().forPath(BASE_PATH);

    worker = new Worker(
        "http",
        "worker",
        "localhost",
        3,
        "0",
        WorkerConfig.DEFAULT_CATEGORY
    );

    workerCuratorCoordinator = new WorkerCuratorCoordinator(
        jsonMapper,
        new IndexerZkConfig(
            new ZkPathsConfig()
            {
              @Override
              public String getBase()
              {
                return BASE_PATH;
              }
            }, null, null, null, null
        ),
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        cf,
        new NodeAnnouncer(cf, Execs.directExecutor()),
        worker
    );
    workerCuratorCoordinator.start();


    // Start a task monitor
    workerTaskMonitor = createTaskMonitor();
    TestTasks.registerSubtypes(jsonMapper);
    jsonMapper.registerSubtypes(new NamedType(TestIndexTask.class, "test_index"));
    workerTaskMonitor.start();

    task = TestTasks.immediateSuccess("test");
  }

  private WorkerTaskMonitor createTaskMonitor()
  {
    final TaskConfig taskConfig = new TaskConfigBuilder()
        .setBaseDir(FileUtils.createTempDir().toString())
        .build();

    TaskActionClientFactory taskActionClientFactory = EasyMock.createNiceMock(TaskActionClientFactory.class);
    TaskActionClient taskActionClient = EasyMock.createNiceMock(TaskActionClient.class);
    EasyMock.expect(taskActionClientFactory.create(EasyMock.anyObject())).andReturn(taskActionClient).anyTimes();
    SegmentHandoffNotifierFactory notifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    EasyMock.replay(taskActionClientFactory, taskActionClient, notifierFactory);
    return new WorkerTaskMonitor(
        jsonMapper,
        new SingleTaskBackgroundRunner(
            new TaskToolboxFactory(
                null,
                taskConfig,
                null,
                taskActionClientFactory,
                null,
                NoopPolicyEnforcer.instance(),
                null,
                null,
                null,
                null,
                null,
                null,
                notifierFactory,
                null,
                null,
                null,
                NoopJoinableFactory.INSTANCE,
                null,
                new SegmentCacheManagerFactory(TestIndex.INDEX_IO, jsonMapper),
                jsonMapper,
                indexIO,
                null,
                null,
                null,
                indexMergerV9Factory,
                null,
                null,
                null,
                null,
                new NoopTestTaskReportFileWriter(),
                null,
                AuthTestUtils.TEST_AUTHORIZER_MAPPER,
                new NoopChatHandlerProvider(),
                testUtils.getRowIngestionMetersFactory(),
                new TestAppenderatorsManager(),
                new NoopOverlordClient(),
                new NoopCoordinatorClient(),
                null,
                null,
                null,
                "1",
                CentralizedDatasourceSchemaConfig.create(),
                JvmUtils.getRuntimeInfo()
            ),
            taskConfig,
            new NoopServiceEmitter(),
            DUMMY_NODE,
            new ServerConfig()
        ),
        taskConfig,
        cf,
        workerCuratorCoordinator,
        EasyMock.createNiceMock(OverlordClient.class)
    );
  }

  @After
  public void tearDown() throws Exception
  {
    workerCuratorCoordinator.stop();
    workerTaskMonitor.stop();
    cf.close();
    testingCluster.stop();
  }

  @Test(timeout = 60_000L)
  public void testRunTask() throws Exception
  {
    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  return cf.checkExists().forPath(JOINER.join(TASKS_PATH, task.getId())) == null;
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );

    cf.create()
      .creatingParentsIfNeeded()
      .forPath(JOINER.join(TASKS_PATH, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  final byte[] bytes = cf.getData().forPath(JOINER.join(STATUS_PATH, task.getId()));
                  final TaskAnnouncement announcement = jsonMapper.readValue(
                      bytes,
                      TaskAnnouncement.class
                  );
                  return announcement.getTaskStatus().isComplete();
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );

    TaskAnnouncement taskAnnouncement = jsonMapper.readValue(
        cf.getData().forPath(JOINER.join(STATUS_PATH, task.getId())), TaskAnnouncement.class
    );

    Assert.assertEquals(task.getId(), taskAnnouncement.getTaskStatus().getId());
    Assert.assertEquals(TaskState.SUCCESS, taskAnnouncement.getTaskStatus().getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testGetAnnouncements() throws Exception
  {
    cf.create()
      .creatingParentsIfNeeded()
      .forPath(JOINER.join(TASKS_PATH, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  final byte[] bytes = cf.getData().forPath(JOINER.join(STATUS_PATH, task.getId()));
                  final TaskAnnouncement announcement = jsonMapper.readValue(
                      bytes,
                      TaskAnnouncement.class
                  );
                  return announcement.getTaskStatus().isComplete();
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );

    List<TaskAnnouncement> announcements = workerCuratorCoordinator.getAnnouncements();
    Assert.assertEquals(1, announcements.size());
    Assert.assertEquals(task.getId(), announcements.get(0).getTaskStatus().getId());
    Assert.assertEquals(TaskState.SUCCESS, announcements.get(0).getTaskStatus().getStatusCode());
    Assert.assertEquals(DUMMY_NODE.getHost(), announcements.get(0).getTaskLocation().getHost());
    Assert.assertEquals(DUMMY_NODE.getPlaintextPort(), announcements.get(0).getTaskLocation().getPort());
  }

  @Test(timeout = 60_000L)
  public void testRestartCleansOldStatus() throws Exception
  {
    task = TestTasks.unending("test");

    cf.create()
      .creatingParentsIfNeeded()
      .forPath(JOINER.join(TASKS_PATH, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  return cf.checkExists().forPath(JOINER.join(STATUS_PATH, task.getId())) != null;
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );
    // simulate node restart
    workerTaskMonitor.stop();
    workerTaskMonitor = createTaskMonitor();
    workerTaskMonitor.start();
    List<TaskAnnouncement> announcements = workerCuratorCoordinator.getAnnouncements();
    Assert.assertEquals(1, announcements.size());
    Assert.assertEquals(task.getId(), announcements.get(0).getTaskStatus().getId());
    Assert.assertEquals(TaskState.FAILED, announcements.get(0).getTaskStatus().getStatusCode());
    Assert.assertEquals(
        "Canceled as unknown task. See middleManager or indexer logs for more details.",
        announcements.get(0).getTaskStatus().getErrorMsg()
    );
  }

  @Test(timeout = 60_000L)
  public void testStatusAnnouncementsArePersistent() throws Exception
  {
    cf.create()
      .creatingParentsIfNeeded()
      .forPath(JOINER.join(TASKS_PATH, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  return cf.checkExists().forPath(JOINER.join(STATUS_PATH, task.getId())) != null;
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );
    // ephemeral owner is 0 is created node is PERSISTENT
    Assert.assertEquals(0, cf.checkExists().forPath(JOINER.join(STATUS_PATH, task.getId())).getEphemeralOwner());

  }
}

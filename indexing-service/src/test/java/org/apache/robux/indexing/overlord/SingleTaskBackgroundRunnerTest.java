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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.robux.client.coordinator.NoopCoordinatorClient;
import org.apache.robux.indexer.TaskLocation;
import org.apache.robux.indexer.TaskState;
import org.apache.robux.indexer.TaskStatus;
import org.apache.robux.indexer.report.SingleFileTaskReportFileWriter;
import org.apache.robux.indexing.common.SegmentCacheManagerFactory;
import org.apache.robux.indexing.common.TaskToolbox;
import org.apache.robux.indexing.common.TaskToolboxFactory;
import org.apache.robux.indexing.common.TestUtils;
import org.apache.robux.indexing.common.actions.TaskActionClient;
import org.apache.robux.indexing.common.actions.TaskActionClientFactory;
import org.apache.robux.indexing.common.config.TaskConfig;
import org.apache.robux.indexing.common.config.TaskConfigBuilder;
import org.apache.robux.indexing.common.task.AbstractTask;
import org.apache.robux.indexing.common.task.NoopTask;
import org.apache.robux.indexing.common.task.TestAppenderatorsManager;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.java.util.emitter.EmittingLogger;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.query.scan.ScanResultValue;
import org.apache.robux.query.spec.MultipleIntervalSegmentSpec;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.join.NoopJoinableFactory;
import org.apache.robux.segment.loading.NoopDataSegmentArchiver;
import org.apache.robux.segment.loading.NoopDataSegmentKiller;
import org.apache.robux.segment.loading.NoopDataSegmentMover;
import org.apache.robux.segment.loading.NoopDataSegmentPusher;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.segment.realtime.NoopChatHandlerProvider;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.SetAndVerifyContextQueryRunner;
import org.apache.robux.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.server.security.AuthTestUtils;
import org.apache.robux.utils.JvmUtils;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class SingleTaskBackgroundRunnerTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private SingleTaskBackgroundRunner runner;

  @Before
  public void setup() throws IOException
  {
    final TestUtils utils = new TestUtils();
    final RobuxNode node = new RobuxNode("testServer", "testHost", false, 1000, null, true, false);
    final TaskConfig taskConfig = new TaskConfigBuilder()
        .setBaseDir(temporaryFolder.newFile().toString())
        .setRestoreTasksOnRestart(true)
        .build();
    final ServiceEmitter emitter = new NoopServiceEmitter();
    EmittingLogger.registerEmitter(emitter);
    final TaskToolboxFactory toolboxFactory = new TaskToolboxFactory(
        null,
        taskConfig,
        null,
        EasyMock.createMock(TaskActionClientFactory.class),
        emitter,
        NoopPolicyEnforcer.instance(),
        new NoopDataSegmentPusher(),
        new NoopDataSegmentKiller(),
        new NoopDataSegmentMover(),
        new NoopDataSegmentArchiver(),
        new NoopDataSegmentAnnouncer(),
        null,
        null,
        null,
        RobuxProcessingConfig::new,
        null,
        NoopJoinableFactory.INSTANCE,
        null,
        new SegmentCacheManagerFactory(TestIndex.INDEX_IO, utils.getTestObjectMapper()),
        utils.getTestObjectMapper(),
        utils.getTestIndexIO(),
        null,
        null,
        null,
        utils.getIndexMergerV9Factory(),
        null,
        node,
        null,
        null,
        new SingleFileTaskReportFileWriter(new File("fake")),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        utils.getRowIngestionMetersFactory(),
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
    runner = new SingleTaskBackgroundRunner(
        toolboxFactory,
        taskConfig,
        emitter,
        node,
        new ServerConfig()
    );
  }

  @After
  public void teardown()
  {
    runner.stop();
  }

  @Test
  public void testRun() throws ExecutionException, InterruptedException
  {
    NoopTask task = new NoopTask(null, null, null, 500L, 0, null);
    Assert.assertEquals(
        TaskState.SUCCESS,
        runner.run(task).get().getStatusCode()
    );
  }

  @Test
  public void testGetQueryRunner() throws ExecutionException, InterruptedException
  {
    runner.run(new NoopTask(null, null, "foo", 500L, 0, null)).get().getStatusCode();

    final QueryRunner<ScanResultValue> queryRunner =
        Robuxs.newScanQueryBuilder()
            .dataSource("foo")
            .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
            .build()
            .getRunner(runner);

    Assert.assertThat(queryRunner, CoreMatchers.instanceOf(SetAndVerifyContextQueryRunner.class));
  }

  @Test
  public void testStop() throws ExecutionException, InterruptedException, TimeoutException
  {
    AtomicReference<Boolean> methodCallHolder = new AtomicReference<>();
    final ListenableFuture<TaskStatus> future = runner.run(
        new NoopTask(null, null, null, Long.MAX_VALUE, 0, null) // infinite task
        {
          @Override
          public boolean waitForCleanupToFinish()
          {
            methodCallHolder.set(true);
            return true;
          }
        }
    );
    runner.stop();
    Assert.assertEquals(
        TaskState.FAILED,
        future.get(1000, TimeUnit.MILLISECONDS).getStatusCode()
    );
    Assert.assertTrue(methodCallHolder.get());
  }

  @Test
  public void testStopWithRestorableTask() throws InterruptedException, ExecutionException, TimeoutException
  {
    final BooleanHolder holder = new BooleanHolder();
    final ListenableFuture<TaskStatus> future = runner.run(
        new RestorableTask(holder)
    );
    runner.stop();
    Assert.assertEquals(
        TaskState.SUCCESS,
        future.get(1000, TimeUnit.MILLISECONDS).getStatusCode()
    );
    Assert.assertTrue(holder.get());
  }

  @Test
  public void testStopRestorableTaskExceptionAfterStop()
  {
    // statusChanged callback can be called by multiple threads.
    AtomicReference<TaskStatus> statusHolder = new AtomicReference<>();
    runner.registerListener(
        new TaskRunnerListener()
        {
          @Override
          public String getListenerId()
          {
            return "testStopRestorableTaskExceptionAfterStop";
          }

          @Override
          public void locationChanged(String taskId, TaskLocation newLocation)
          {
            // do nothing
          }

          @Override
          public void statusChanged(String taskId, TaskStatus status)
          {
            statusHolder.set(status);
          }
        },
        Execs.directExecutor()
    );
    runner.run(
        new RestorableTask(new BooleanHolder())
        {
          @Override
          public TaskStatus runTask(TaskToolbox toolbox)
          {
            throw new Error("task failure test");
          }

          @Nullable
          @Override
          public String setup(TaskToolbox toolbox)
          {
            return null;
          }

          @Override
          public void cleanUp(TaskToolbox toolbox, TaskStatus taskStatus)
          {
            // do nothing
          }
        }
    );
    runner.stop();
    Assert.assertEquals(TaskState.FAILED, statusHolder.get().getStatusCode());
    Assert.assertEquals(
        "Failed to stop gracefully with exception. See task logs for more details.",
        statusHolder.get().getErrorMsg()
    );
  }

  @Test
  public void testStopNonRestorableTask() throws InterruptedException
  {
    // latch to wait for SingleTaskBackgroundRunnerCallable to be executed before stopping the task
    // We need this latch because TaskRunnerListener is currently racy.
    // See https://github.com/apache/robux/issues/11445 for more details.
    CountDownLatch runLatch = new CountDownLatch(1);
    // statusChanged callback can be called by multiple threads.
    AtomicReference<TaskStatus> statusHolder = new AtomicReference<>();
    runner.registerListener(
        new TaskRunnerListener()
        {
          @Override
          public String getListenerId()
          {
            return "testStopNonRestorableTask";
          }

          @Override
          public void locationChanged(String taskId, TaskLocation newLocation)
          {
            // do nothing
          }

          @Override
          public void statusChanged(String taskId, TaskStatus status)
          {
            if (status.getStatusCode() == TaskState.RUNNING) {
              runLatch.countDown();
            } else {
              statusHolder.set(status);
            }
          }
        },
        Execs.directExecutor()
    );
    runner.run(
        new NoopTask(
            null,
            null,
            "datasource",
            10000, // 10 sec
            0,
            null
        )
        {
          @Override
          public boolean waitForCleanupToFinish()
          {
            return true;
          }
        }
    );

    Assert.assertTrue(runLatch.await(1, TimeUnit.SECONDS));
    runner.stop();

    Assert.assertEquals(TaskState.FAILED, statusHolder.get().getStatusCode());
    Assert.assertEquals(
        "Canceled as task execution process stopped",
        statusHolder.get().getErrorMsg()
    );
  }

  private static class RestorableTask extends AbstractTask
  {
    private final BooleanHolder gracefullyStopped;

    RestorableTask(BooleanHolder gracefullyStopped)
    {
      super("testId", "testDataSource", Collections.emptyMap());

      this.gracefullyStopped = gracefullyStopped;
    }

    @Override
    public String getType()
    {
      return "restorable";
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient)
    {
      return true;
    }

    @Override
    public TaskStatus runTask(TaskToolbox toolbox)
    {
      return TaskStatus.success(getId());
    }

    @Override
    public boolean canRestore()
    {
      return true;
    }

    @Override
    public void stopGracefully(TaskConfig taskConfig)
    {
      gracefullyStopped.set();
    }

    @Nullable
    @Override
    public String setup(TaskToolbox toolbox)
    {
      return null;
    }

    @Override
    public void cleanUp(TaskToolbox toolbox, TaskStatus taskStatus)
    {
      // do nothing
    }


  }

  private static class BooleanHolder
  {
    private boolean value;

    void set()
    {
      this.value = true;
    }

    boolean get()
    {
      return value;
    }
  }
}

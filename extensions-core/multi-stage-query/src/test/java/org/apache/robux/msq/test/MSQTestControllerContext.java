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

package org.apache.robux.msq.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import org.apache.robux.client.ImmutableSegmentLoadInfo;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.client.indexing.TaskStatusResponse;
import org.apache.robux.common.guava.FutureUtils;
import org.apache.robux.indexer.RunnerTaskState;
import org.apache.robux.indexer.TaskLocation;
import org.apache.robux.indexer.TaskState;
import org.apache.robux.indexer.TaskStatus;
import org.apache.robux.indexer.TaskStatusPlus;
import org.apache.robux.indexing.common.TaskLockType;
import org.apache.robux.indexing.common.actions.TaskActionClient;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.FileUtils;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.msq.dart.controller.DartControllerContextFactory;
import org.apache.robux.msq.exec.Controller;
import org.apache.robux.msq.exec.ControllerContext;
import org.apache.robux.msq.exec.ControllerMemoryParameters;
import org.apache.robux.msq.exec.MSQMetriceEventBuilder;
import org.apache.robux.msq.exec.SegmentSource;
import org.apache.robux.msq.exec.Worker;
import org.apache.robux.msq.exec.WorkerClient;
import org.apache.robux.msq.exec.WorkerFailureListener;
import org.apache.robux.msq.exec.WorkerImpl;
import org.apache.robux.msq.exec.WorkerManager;
import org.apache.robux.msq.exec.WorkerMemoryParameters;
import org.apache.robux.msq.exec.WorkerRunRef;
import org.apache.robux.msq.exec.WorkerStorageParameters;
import org.apache.robux.msq.indexing.IndexerControllerContext;
import org.apache.robux.msq.indexing.IndexerTableInputSpecSlicer;
import org.apache.robux.msq.indexing.MSQSpec;
import org.apache.robux.msq.indexing.MSQWorkerTask;
import org.apache.robux.msq.indexing.MSQWorkerTaskLauncher;
import org.apache.robux.msq.indexing.MSQWorkerTaskLauncher.MSQWorkerTaskLauncherConfig;
import org.apache.robux.msq.input.InputSpecSlicer;
import org.apache.robux.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.robux.msq.util.MultiStageQueryContext;
import org.apache.robux.query.QueryContext;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.server.RobuxNode;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class MSQTestControllerContext implements ControllerContext, DartControllerContextFactory
{
  private static final Logger log = new Logger(MSQTestControllerContext.class);
  private static final int NUM_WORKERS = 4;
  private final TaskActionClient taskActionClient;
  private final ConcurrentHashMap<String, WorkerRunRef> inMemoryWorkers = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, TaskStatus> statusMap = new ConcurrentHashMap<>();
  private static final ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator(Execs.multiThreaded(
      NUM_WORKERS,
      "MultiStageQuery-test-controller-client"
  ));
  private final File tempDir = FileUtils.createTempDir();
  private final CoordinatorClient coordinatorClient;
  private final RobuxNode node = new RobuxNode(
      "controller",
      "localhost",
      true,
      8080,
      8081,
      true,
      false
  );
  private final Injector injector;
  private final String queryId;
  private final ObjectMapper mapper;

  private Controller controller;
  private final WorkerMemoryParameters workerMemoryParameters;
  private final TaskLockType taskLockType;
  private final ServiceEmitter serviceEmitter;
  private QueryContext queryContext;

  public MSQTestControllerContext(
      String queryId,
      ObjectMapper mapper,
      Injector injector,
      TaskActionClient taskActionClient,
      WorkerMemoryParameters workerMemoryParameters,
      List<ImmutableSegmentLoadInfo> loadedSegments,
      TaskLockType taskLockType,
      QueryContext queryContext,
      ServiceEmitter serviceEmitter
  )
  {
    this.queryId = queryId;
    this.mapper = mapper;
    this.injector = injector;
    this.taskActionClient = taskActionClient;
    this.serviceEmitter = serviceEmitter;
    coordinatorClient = Mockito.mock(CoordinatorClient.class);

    Mockito.when(coordinatorClient.fetchServerViewSegments(
                     ArgumentMatchers.anyString(),
                     ArgumentMatchers.any()
                 )
    ).thenAnswer(invocation -> loadedSegments.stream()
                                             .filter(immutableSegmentLoadInfo ->
                                                         immutableSegmentLoadInfo.getSegment()
                                                                                 .getDataSource()
                                                                                 .equals(invocation.getArguments()[0]))
                                             .collect(Collectors.toList())
    );
    this.workerMemoryParameters = workerMemoryParameters;
    this.taskLockType = taskLockType;
    this.queryContext = queryContext;
  }

  OverlordClient overlordClient = new NoopOverlordClient()
  {
    @Override
    public ListenableFuture<Void> runTask(String taskId, Object taskObject)
    {
      final MSQWorkerTask task = (MSQWorkerTask) taskObject;
      if (controller == null) {
        throw new ISE("Controller needs to be set using the register method");
      }

      WorkerStorageParameters workerStorageParameters;
      // If we are testing durable storage, set a low limit on storage so that the durable storage will be used.
      if (MultiStageQueryContext.isDurableStorageEnabled(QueryContext.of(task.getContext()))) {
        workerStorageParameters = WorkerStorageParameters.createInstanceForTests(100);
      } else {
        workerStorageParameters = WorkerStorageParameters.createInstanceForTests(Long.MAX_VALUE);
      }

      Worker worker = new WorkerImpl(
          task,
          new MSQTestWorkerContext(
              task.getId(),
              inMemoryWorkers,
              controller,
              mapper,
              injector,
              workerMemoryParameters,
              workerStorageParameters,
              serviceEmitter
          )
      );
      final WorkerRunRef workerRunRef = new WorkerRunRef();
      ListenableFuture<?> future = workerRunRef.run(worker, EXECUTOR);
      inMemoryWorkers.put(task.getId(), workerRunRef);
      statusMap.put(task.getId(), TaskStatus.running(task.getId()));

      Futures.addCallback(future, new FutureCallback<Object>()
      {
        @Override
        public void onSuccess(@Nullable Object result)
        {
          statusMap.put(task.getId(), TaskStatus.success(task.getId()));
        }

        @Override
        public void onFailure(Throwable t)
        {
          log.error(t, "error running worker task %s", task.getId());
          statusMap.put(task.getId(), TaskStatus.failure(task.getId(), t.getMessage()));
        }
      }, MoreExecutors.directExecutor());

      return Futures.immediateFuture(null);
    }

    @Override
    public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
    {
      Map<String, TaskStatus> result = new HashMap<>();
      for (String taskId : taskIds) {
        TaskStatus taskStatus = statusMap.get(taskId);
        if (taskStatus != null) {

          if (taskStatus.getStatusCode().equals(TaskState.RUNNING) && !inMemoryWorkers.containsKey(taskId)) {
            result.put(taskId, new TaskStatus(taskId, TaskState.FAILED, 0, null, null));
          } else {
            result.put(
                taskId,
                new TaskStatus(
                    taskStatus.getId(),
                    taskStatus.getStatusCode(),
                    taskStatus.getDuration(),
                    taskStatus.getErrorMsg(),
                    taskStatus.getLocation()
                )
            );
          }
        }
      }
      return Futures.immediateFuture(result);
    }

    @Override
    public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
    {
      final Map<String, TaskStatus> taskStatusMap =
          FutureUtils.getUnchecked(taskStatuses(Collections.singleton(taskId)), true);

      final TaskStatus taskStatus = taskStatusMap.get(taskId);
      if (taskStatus == null) {
        return Futures.immediateFuture(new TaskStatusResponse(taskId, null));
      } else {
        return Futures.immediateFuture(
            new TaskStatusResponse(
                taskId,
                new TaskStatusPlus(
                    taskStatus.getId(),
                    null,
                    null,
                    DateTimes.utc(0),
                    DateTimes.utc(0),
                    taskStatus.getStatusCode(),
                    taskStatus.getStatusCode(),
                    taskStatus.getStatusCode().isRunnable() ? RunnerTaskState.RUNNING : RunnerTaskState.NONE,
                    null,
                    taskStatus.getStatusCode().isRunnable()
                    ? TaskLocation.create("host-" + taskId, 1, -1)
                    : TaskLocation.unknown(),
                    null,
                    taskStatus.getErrorMsg()
                )
            )
        );
      }
    }

    @Override
    public ListenableFuture<Void> cancelTask(String workerId)
    {
      final WorkerRunRef workerRunRef = inMemoryWorkers.remove(workerId);
      if (workerRunRef != null) {
        workerRunRef.cancel();
      }
      return Futures.immediateFuture(null);
    }
  };

  @Override
  public String queryId()
  {
    return queryId;
  }

  @Override
  public ControllerQueryKernelConfig queryKernelConfig(MSQSpec querySpec)
  {
    return IndexerControllerContext.makeQueryKernelConfig(querySpec, new ControllerMemoryParameters(100_000_000));
  }

  @Override
  public void emitMetric(MSQMetriceEventBuilder metricBuilder)
  {
    serviceEmitter.emit(
        metricBuilder.setDimension(
            MSQTestOverlordServiceClient.TEST_METRIC_DIMENSION,
            MSQTestOverlordServiceClient.METRIC_CONTROLLER_TASK_TYPE
        )
    );
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return mapper;
  }

  @Override
  public Injector injector()
  {
    return injector;
  }

  @Override
  public RobuxNode selfNode()
  {
    return node;
  }

  @Override
  public TaskActionClient taskActionClient()
  {
    return taskActionClient;
  }

  @Override
  public TaskLockType taskLockType()
  {
    return taskLockType;
  }

  @Override
  public InputSpecSlicer newTableInputSpecSlicer(WorkerManager workerManager)
  {
    return new IndexerTableInputSpecSlicer(
        coordinatorClient,
        taskActionClient,
        MultiStageQueryContext.getSegmentSources(queryContext, SegmentSource.NONE)
    );
  }

  @Override
  public WorkerManager newWorkerManager(
      String queryId,
      MSQSpec querySpec,
      ControllerQueryKernelConfig queryKernelConfig,
      WorkerFailureListener workerFailureListener
  )
  {
    MSQWorkerTaskLauncherConfig taskLauncherConfig = new MSQWorkerTaskLauncherConfig();
    taskLauncherConfig.highFrequencyCheckMillis = 1;
    taskLauncherConfig.switchToLowFrequencyCheckAfterMillis = 25;
    taskLauncherConfig.lowFrequencyCheckMillis = 2;

    return new MSQWorkerTaskLauncher(
        controller.queryId(),
        "test-datasource",
        overlordClient,
        workerFailureListener,
        IndexerControllerContext.makeTaskContext(querySpec, queryKernelConfig, ImmutableMap.of()),
        0,
        taskLauncherConfig
    );
  }

  public QueryContext getQueryContext()
  {
    return queryContext;
  }

  @Override
  public File taskTempDir()
  {
    return tempDir;
  }

  @Override
  public void registerController(Controller controller, Closer closer)
  {
    this.controller = controller;
  }

  @Override
  public WorkerClient newWorkerClient()
  {
    return new MSQTestWorkerClient(inMemoryWorkers);
  }

  @Override
  public ControllerContext newContext(QueryContext context)
  {
    this.queryContext = this.queryContext.override(context);
    return this;
  }
}

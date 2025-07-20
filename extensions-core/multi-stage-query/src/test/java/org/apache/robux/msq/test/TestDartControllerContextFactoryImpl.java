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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.guice.annotations.Smile;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.java.util.metrics.StubServiceEmitter;
import org.apache.robux.msq.dart.Dart;
import org.apache.robux.msq.dart.controller.DartControllerContext;
import org.apache.robux.msq.dart.controller.DartControllerContextFactoryImpl;
import org.apache.robux.msq.dart.worker.DartWorkerClient;
import org.apache.robux.msq.exec.Controller;
import org.apache.robux.msq.exec.ControllerContext;
import org.apache.robux.msq.exec.MSQMetriceEventBuilder;
import org.apache.robux.msq.exec.MemoryIntrospector;
import org.apache.robux.msq.exec.Worker;
import org.apache.robux.msq.exec.WorkerImpl;
import org.apache.robux.msq.exec.WorkerRunRef;
import org.apache.robux.msq.exec.WorkerStorageParameters;
import org.apache.robux.msq.kernel.StageId;
import org.apache.robux.msq.kernel.WorkOrder;
import org.apache.robux.query.QueryContext;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.server.RobuxNode;

import java.util.Map;
import java.util.concurrent.Executors;

public class TestDartControllerContextFactoryImpl extends DartControllerContextFactoryImpl
{
  private static final ListeningExecutorService EXECUTOR = MoreExecutors.listeningDecorator(
      Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setNameFormat("dart-worker-%d").build()
      )
  );

  private final Map<String, WorkerRunRef> workerMap;
  public Controller controller;
  private final ServiceEmitter serviceEmitter = new StubServiceEmitter();

  @Inject
  public TestDartControllerContextFactoryImpl(
      final Injector injector,
      @Json final ObjectMapper jsonMapper,
      @Smile final ObjectMapper smileMapper,
      @Self final RobuxNode selfNode,
      @EscalatedGlobal final ServiceClientFactory serviceClientFactory,
      final MemoryIntrospector memoryIntrospector,
      final TimelineServerView serverView,
      final ServiceEmitter emitter,
      @Dart Map<String, WorkerRunRef> workerMap
  )
  {
    super(injector, jsonMapper, smileMapper, selfNode, serviceClientFactory, memoryIntrospector, serverView, emitter);
    this.workerMap = workerMap;
  }

  @Override
  public ControllerContext newContext(QueryContext context)
  {
    return new DartControllerContext(
        injector,
        jsonMapper,
        selfNode,
        new DartTestWorkerClient(),
        memoryIntrospector,
        serverView,
        emitter,
        context
    )
    {
      @Override
      public void registerController(Controller currentController, Closer closer)
      {
        super.registerController(currentController, closer);
        controller = currentController;
      }

      @Override
      public void emitMetric(MSQMetriceEventBuilder metricBuilder)
      {
        serviceEmitter.emit(metricBuilder.build("controller", queryId()));
      }
    };
  }

  public class DartTestWorkerClient extends MSQTestWorkerClient implements DartWorkerClient
  {

    public DartTestWorkerClient()
    {
      super(workerMap);
    }

    @Override
    protected WorkerRunRef newWorker(String workerId)
    {
      final Worker worker = new WorkerImpl(
          null,
          new MSQTestWorkerContext(
              workerId,
              inMemoryWorkers,
              controller,
              jsonMapper,
              injector,
              MSQTestBase.makeTestWorkerMemoryParameters(),
              WorkerStorageParameters.createInstanceForTests(Long.MAX_VALUE),
              serviceEmitter
          )
      );
      final WorkerRunRef workerRunRef = new WorkerRunRef();
      workerRunRef.run(worker, EXECUTOR)
                  .addListener(() -> inMemoryWorkers.remove(workerId), MoreExecutors.directExecutor());
      return workerRunRef;
    }

    @Override
    public ListenableFuture<Void> postWorkOrder(String workerTaskId, WorkOrder workOrder)
    {
      return super.postWorkOrder(workerTaskId, workOrder);
    }

    @Override
    public ListenableFuture<Void> postCleanupStage(String workerTaskId, StageId stageId)
    {
      return super.postCleanupStage(workerTaskId, stageId);

    }

    @Override
    public void closeClient(String hostAndPort)
    {
    }

    @Override
    public ListenableFuture<?> stopWorker(String workerId)
    {
      return null;
    }
  }
}

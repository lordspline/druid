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

import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.client.coordinator.NoopCoordinatorClient;
import org.apache.robux.collections.NonBlockingPool;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Merging;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.msq.dart.Dart;
import org.apache.robux.msq.dart.controller.DartControllerContextFactory;
import org.apache.robux.msq.dart.controller.sql.DartSqlEngine;
import org.apache.robux.msq.dart.guice.DartControllerModule;
import org.apache.robux.msq.dart.guice.DartModules;
import org.apache.robux.msq.dart.guice.DartWorkerMemoryManagementModule;
import org.apache.robux.msq.dart.guice.DartWorkerModule;
import org.apache.robux.msq.exec.WorkerRunRef;
import org.apache.robux.query.TestBufferPool;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.rpc.guice.ServiceClientModule;
import org.apache.robux.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.robux.sql.avatica.DartRobuxMeta;
import org.apache.robux.sql.avatica.RobuxMeta;
import org.apache.robux.sql.calcite.TempDirProducer;
import org.apache.robux.sql.calcite.run.SqlEngine;
import org.apache.robux.sql.calcite.util.CalciteTests;
import org.apache.robux.sql.calcite.util.RobuxModuleCollection;
import org.apache.robux.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.robux.sql.calcite.util.datasets.TestDataSet;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class DartComponentSupplier extends AbstractMSQComponentSupplierDelegate
{
  public DartComponentSupplier(TempDirProducer tempFolderProducer)
  {
    super(new StandardComponentSupplier(tempFolderProducer));
  }

  @Override
  public void gatherProperties(Properties properties)
  {
    super.gatherProperties(properties);
    properties.put(DartModules.DART_ENABLED_PROPERTY, "true");
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
  {
    walker.add(TestDataSet.NUMBERS, getTempDirProducer().newTempFolder("tmp_numbers"));
    return super.addSegmentsToWalker(walker);
  }
  @Override
  public RobuxModule getCoreModule()
  {
    return RobuxModuleCollection.of(
        super.getCoreModule(),
        new DartControllerModule(),
        new DartWorkerModule(),
        new DartWorkerMemoryManagementModule(),
        new DartTestCoreModule()
    );
  }

  @Override
  public RobuxModule getOverrideModule()
  {
    return RobuxModuleCollection.of(
        super.getOverrideModule(),
        new DartTestOverrideModule()
    );
  }

  @Override
  public Class<? extends SqlEngine> getSqlEngineClass()
  {
    return DartSqlEngine.class;
  }

  static class DartTestCoreModule implements RobuxModule
  {
    @Provides
    @EscalatedGlobal
    final ServiceClientFactory getServiceClientFactory(HttpClient ht)
    {
      return ServiceClientModule.makeServiceClientFactory(ht);

    }

    @Provides
    final RobuxNodeDiscoveryProvider getDiscoveryProvider()
    {
      return new CalciteTests.FakeRobuxNodeDiscoveryProvider(Collections.emptyMap());
    }

    @Override
    public void configure(Binder binder)
    {
      binder.bind(CoordinatorClient.class).to(NoopCoordinatorClient.class);
    }
  }

  static class DartTestOverrideModule implements RobuxModule
  {
    @Provides
    @LazySingleton
    public RobuxMeta createMeta(DartRobuxMeta robuxMeta)
    {
      return robuxMeta;
    }

    @Override
    public void configure(Binder binder)
    {
      binder.bind(DartControllerContextFactory.class)
          .to(TestDartControllerContextFactoryImpl.class)
          .in(LazySingleton.class);
    }

    @Provides
    @Merging
    NonBlockingPool<ByteBuffer> makeMergingBuffer(TestBufferPool bufferPool)
    {
      return bufferPool;
    }

    @Provides
    @LazySingleton
    @Dart
    Map<String, WorkerRunRef> workerMap()
    {
      return new ConcurrentHashMap<>();
    }
  }
}

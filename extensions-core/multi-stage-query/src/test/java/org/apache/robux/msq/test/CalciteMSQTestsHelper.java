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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import org.apache.robux.collections.ReferenceCountingResourceHolder;
import org.apache.robux.collections.ResourceHolder;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.frame.processor.Bouncer;
import org.apache.robux.guice.IndexingServiceTuningConfigModule;
import org.apache.robux.guice.JoinableFactoryModule;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.indexing.common.SegmentCacheManagerFactory;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.msq.counters.ChannelCounters;
import org.apache.robux.msq.exec.DataServerQueryHandler;
import org.apache.robux.msq.exec.DataServerQueryHandlerFactory;
import org.apache.robux.msq.guice.MSQExternalDataSourceModule;
import org.apache.robux.msq.guice.MSQIndexingModule;
import org.apache.robux.msq.querykit.DataSegmentProvider;
import org.apache.robux.query.ForwardingQueryProcessingPool;
import org.apache.robux.query.QueryProcessingPool;
import org.apache.robux.query.groupby.TestGroupByBuffers;
import org.apache.robux.segment.CompleteSegment;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.loading.DataSegmentPusher;
import org.apache.robux.segment.loading.LocalDataSegmentPusher;
import org.apache.robux.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.robux.segment.loading.SegmentCacheManager;
import org.apache.robux.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.robux.server.SegmentManager;
import org.apache.robux.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.robux.server.coordination.DataSegmentAnnouncer;
import org.apache.robux.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.robux.sql.calcite.TempDirProducer;
import org.apache.robux.timeline.SegmentId;
import org.easymock.EasyMock;
import org.mockito.Mockito;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;

/**
 * Helper class aiding in wiring up the Guice bindings required for MSQ engine to work with the Calcite's tests
 */
public class CalciteMSQTestsHelper
{
  public static final class MSQTestModule implements RobuxModule
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bind(AppenderatorsManager.class).toProvider(() -> null);

      // Requirements of JoinableFactoryModule
      binder.bind(SegmentManager.class).toInstance(EasyMock.createMock(SegmentManager.class));

      binder.bind(new TypeLiteral<Set<NodeRole>>()
      {
      }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.PEON));

      binder.bind(QueryProcessingPool.class)
            .toInstance(new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool")));

      binder.bind(Bouncer.class).toInstance(new Bouncer(1));
    }

    @Provides
    public SegmentCacheManager provideSegmentCacheManager(ObjectMapper testMapper, TempDirProducer tempDirProducer)
    {
      return new SegmentCacheManagerFactory(TestIndex.INDEX_IO, testMapper)
          .manufacturate(tempDirProducer.newTempFolder("test"));
    }

    @Provides
    public LocalDataSegmentPusherConfig provideLocalDataSegmentPusherConfig(TempDirProducer tempDirProducer)
    {
      LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
      config.storageDirectory = tempDirProducer.newTempFolder("localsegments");
      return config;
    }

    @Provides
    public MSQTestSegmentManager provideMSQTestSegmentManager(SegmentCacheManager segmentCacheManager)
    {
      return new MSQTestSegmentManager(segmentCacheManager);
    }

    @Provides
    public DataSegmentPusher provideDataSegmentPusher(LocalDataSegmentPusherConfig config,
        MSQTestSegmentManager segmentManager)
    {
      return new MSQTestDelegateDataSegmentPusher(new LocalDataSegmentPusher(config), segmentManager);
    }

    @Provides
    public DataSegmentAnnouncer provideDataSegmentAnnouncer()
    {
      return new NoopDataSegmentAnnouncer();
    }

    @Provides
    @LazySingleton
    public DataSegmentProvider provideDataSegmentProvider(LocalDataSegmentProvider localDataSegmentProvider)
    {
      return localDataSegmentProvider;
    }

    @LazySingleton
    static class LocalDataSegmentProvider implements DataSegmentProvider
    {
      private SpecificSegmentsQuerySegmentWalker walker;

      @Inject
      public LocalDataSegmentProvider(SpecificSegmentsQuerySegmentWalker walker)
      {
        this.walker = walker;
      }

      @Override
      public Supplier<ResourceHolder<CompleteSegment>> fetchSegment(
          SegmentId segmentId,
          ChannelCounters channelCounters,
          boolean isReindex)
      {
        CompleteSegment a = walker.getSegment(segmentId);
        return () -> new ReferenceCountingResourceHolder<>(a, Closer.create());
      }
    }

    @Provides
    public DataServerQueryHandlerFactory provideDataServerQueryHandlerFactory()
    {
      return getTestDataServerQueryHandlerFactory();
    }
  }

  @Deprecated
  public static List<Module> fetchModules(
      Function<String, File> tempFolderProducer,
      TestGroupByBuffers groupByBuffers
  )
  {
    return ImmutableList.of(
        new MSQTestModule(),
        new IndexingServiceTuningConfigModule(),
        new JoinableFactoryModule(),
        new MSQExternalDataSourceModule(),
        new MSQIndexingModule()
    );
  }

  private static DataServerQueryHandlerFactory getTestDataServerQueryHandlerFactory()
  {
    // Currently, there is no metadata in this test for loaded segments. Therefore, this should not be called.
    // In the future, if this needs to be supported, mocks for DataServerQueryHandler should be added like
    // org.apache.robux.msq.exec.MSQLoadedSegmentTests.
    DataServerQueryHandlerFactory mockFactory = Mockito.mock(DataServerQueryHandlerFactory.class);
    DataServerQueryHandler dataServerQueryHandler = Mockito.mock(DataServerQueryHandler.class);
    doThrow(new AssertionError("Test does not support loaded segment query"))
        .when(dataServerQueryHandler).fetchRowsFromDataServer(any(), any(), any());
    doReturn(dataServerQueryHandler).when(mockFactory).createDataServerQueryHandler(anyString(), any(), any());
    return mockFactory;
  }
}

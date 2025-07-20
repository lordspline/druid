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

package org.apache.robux.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import org.apache.robux.client.cache.Cache;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.guice.RobuxInjectorBuilder;
import org.apache.robux.guice.ExpressionModule;
import org.apache.robux.guice.SegmentWranglerModule;
import org.apache.robux.guice.StartupInjectorBuilder;
import org.apache.robux.initialization.CoreInjectorBuilder;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.query.BrokerParallelMergeConfig;
import org.apache.robux.query.DataSource;
import org.apache.robux.query.DefaultGenericQueryMetricsFactory;
import org.apache.robux.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.FrameBasedInlineDataSource;
import org.apache.robux.query.InlineDataSource;
import org.apache.robux.query.LookupDataSource;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryRunnerFactory;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QueryRunnerTestHelper;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.RetryQueryRunnerConfig;
import org.apache.robux.query.TestBufferPool;
import org.apache.robux.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.query.groupby.GroupByQueryConfig;
import org.apache.robux.query.groupby.GroupByQueryRunnerFactory;
import org.apache.robux.query.groupby.GroupByQueryRunnerTest;
import org.apache.robux.query.groupby.TestGroupByBuffers;
import org.apache.robux.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.robux.query.metadata.SegmentMetadataQueryConfig;
import org.apache.robux.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.robux.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.robux.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.robux.query.operator.WindowOperatorQuery;
import org.apache.robux.query.operator.WindowOperatorQueryQueryRunnerFactory;
import org.apache.robux.query.operator.WindowOperatorQueryQueryToolChest;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.query.scan.ScanQuery;
import org.apache.robux.query.scan.ScanQueryConfig;
import org.apache.robux.query.scan.ScanQueryEngine;
import org.apache.robux.query.scan.ScanQueryQueryToolChest;
import org.apache.robux.query.scan.ScanQueryRunnerFactory;
import org.apache.robux.query.search.SearchQuery;
import org.apache.robux.query.search.SearchQueryConfig;
import org.apache.robux.query.search.SearchQueryQueryToolChest;
import org.apache.robux.query.search.SearchQueryRunnerFactory;
import org.apache.robux.query.search.SearchStrategySelector;
import org.apache.robux.query.timeboundary.TimeBoundaryQuery;
import org.apache.robux.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import org.apache.robux.query.timeseries.TimeseriesQuery;
import org.apache.robux.query.timeseries.TimeseriesQueryEngine;
import org.apache.robux.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.robux.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.robux.query.topn.TopNQuery;
import org.apache.robux.query.topn.TopNQueryConfig;
import org.apache.robux.query.topn.TopNQueryQueryToolChest;
import org.apache.robux.query.topn.TopNQueryRunnerFactory;
import org.apache.robux.query.union.UnionQuery;
import org.apache.robux.query.union.UnionQueryLogic;
import org.apache.robux.segment.ReferenceCountedSegmentProvider;
import org.apache.robux.segment.SegmentWrangler;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.join.FrameBasedInlineJoinableFactory;
import org.apache.robux.segment.join.InlineJoinableFactory;
import org.apache.robux.segment.join.JoinableFactory;
import org.apache.robux.segment.join.JoinableFactoryWrapper;
import org.apache.robux.segment.join.LookupJoinableFactory;
import org.apache.robux.segment.join.MapJoinableFactory;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.server.metrics.SubqueryCountStatsProvider;
import org.apache.robux.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.robux.server.scheduling.NoQueryLaningStrategy;
import org.apache.robux.sql.calcite.util.CacheTestHelperModule;
import org.apache.robux.sql.calcite.util.CacheTestHelperModule.ResultCacheMode;
import org.apache.robux.timeline.VersionedIntervalTimeline;
import org.apache.robux.utils.JvmUtils;
import org.junit.Assert;
import org.junit.rules.ExternalResource;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Utilities for creating query-stack objects for tests.
 */
public class QueryStackTests
{
  public static class Junit4ConglomerateRule extends ExternalResource
  {
    private Closer closer;
    private QueryRunnerFactoryConglomerate conglomerate;

    @Override
    protected void before()
    {
      closer = Closer.create();
      conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
    }

    @Override
    protected void after()
    {
      try {
        closer.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      conglomerate = null;
      closer = null;
    }

    public QueryRunnerFactoryConglomerate getConglomerate()
    {
      return conglomerate;
    }
  }


  public static final QueryScheduler DEFAULT_NOOP_SCHEDULER = new QueryScheduler(
      0,
      ManualQueryPrioritizationStrategy.INSTANCE,
      NoQueryLaningStrategy.INSTANCE,
      new ServerConfig()
  );

  public static final int DEFAULT_NUM_MERGE_BUFFERS = -1;

  private static final int COMPUTE_BUFFER_SIZE = 10 * 1024 * 1024;

  private QueryStackTests()
  {
    // No instantiation.
  }

  public static ClientQuerySegmentWalker createClientQuerySegmentWalker(
      final Injector injector,
      final QuerySegmentWalker clusterWalker,
      final QuerySegmentWalker localWalker,
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactory joinableFactory,
      final ServerConfig serverConfig,
      final ServiceEmitter emitter
  )
  {
    return new ClientQuerySegmentWalker(
        emitter,
        clusterWalker,
        localWalker,
        conglomerate,
        joinableFactory,
        new RetryQueryRunnerConfig(),
        injector.getInstance(ObjectMapper.class),
        serverConfig,
        injector.getInstance(Cache.class),
        injector.getInstance(CacheConfig.class),
        new SubqueryGuardrailHelper(null, JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(), 1),
        new SubqueryCountStatsProvider(),
        new DefaultGenericQueryMetricsFactory()
    );
  }

  public static TestClusterQuerySegmentWalker createClusterQuerySegmentWalker(
      Map<String, VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>> timelines,
      QueryRunnerFactoryConglomerate conglomerate,
      @Nullable QueryScheduler scheduler,
      Injector injector
  )
  {
    return new TestClusterQuerySegmentWalker(timelines, conglomerate, scheduler, injector.getInstance(EtagProvider.KEY));
  }

  public static LocalQuerySegmentWalker createLocalQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SegmentWrangler segmentWrangler,
      final JoinableFactoryWrapper joinableFactoryWrapper,
      final QueryScheduler scheduler,
      final ServiceEmitter emitter
  )
  {
    return new LocalQuerySegmentWalker(
        conglomerate,
        segmentWrangler,
        joinableFactoryWrapper,
        scheduler,
        NoopPolicyEnforcer.instance(),
        emitter
    );
  }

  public static BrokerParallelMergeConfig getParallelMergeConfig(
      boolean useParallelMergePoolConfigured
  )
  {
    return new BrokerParallelMergeConfig() {
      @Override
      public boolean useParallelMergePool()
      {
        return useParallelMergePoolConfigured;
      }
    };
  }
  public static RobuxProcessingConfig getProcessingConfig(final int mergeBuffers)
  {
    return new RobuxProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return null;
      }

      @Override
      public int intermediateComputeSizeBytes()
      {
        return COMPUTE_BUFFER_SIZE;
      }

      @Override
      public int getNumThreads()
      {
        // Only use 1 thread for tests.
        return 1;
      }

      @Override
      public int getNumMergeBuffers()
      {
        if (mergeBuffers == DEFAULT_NUM_MERGE_BUFFERS) {
          return 2;
        }
        return mergeBuffers;
      }
    };
  }

  /**
   * Returns a new {@link QueryRunnerFactoryConglomerate}. Adds relevant closeables to the passed-in {@link Closer}.
   */
  public static QueryRunnerFactoryConglomerate createQueryRunnerFactoryConglomerate(final Closer closer)
  {
    return createQueryRunnerFactoryConglomerate(closer, TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD);
  }

  public static QueryRunnerFactoryConglomerate createQueryRunnerFactoryConglomerate(
      final Closer closer,
      final Integer minTopNThreshold
  )
  {
    return createQueryRunnerFactoryConglomerate(
        closer,
        getProcessingConfig(
            DEFAULT_NUM_MERGE_BUFFERS
        ),
        minTopNThreshold,
        TestHelper.makeJsonMapper()
    );
  }

  public static QueryRunnerFactoryConglomerate createQueryRunnerFactoryConglomerate(
      final Closer closer,
      final RobuxProcessingConfig processingConfig
  )
  {
    return createQueryRunnerFactoryConglomerate(
        closer,
        processingConfig,
        TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD,
        TestHelper.makeJsonMapper()
    );
  }

  public static TestBufferPool makeTestBufferPool(final Closer closer)
  {
    final TestBufferPool testBufferPool = TestBufferPool.offHeap(COMPUTE_BUFFER_SIZE, Integer.MAX_VALUE);
    closer.register(() -> {
      // Verify that all objects have been returned to the pool.
      Assert.assertEquals(0, testBufferPool.getOutstandingObjectCount());
    });
    return testBufferPool;
  }

  public static TestGroupByBuffers makeGroupByBuffers(final Closer closer, final RobuxProcessingConfig processingConfig)
  {
    final TestGroupByBuffers groupByBuffers =
        closer.register(TestGroupByBuffers.createFromProcessingConfig(processingConfig));
    return groupByBuffers;
  }

  public static QueryRunnerFactoryConglomerate createQueryRunnerFactoryConglomerate(
      final Closer closer,
      final RobuxProcessingConfig processingConfig,
      final Integer minTopNThreshold,
      final ObjectMapper jsonMapper
  )
  {
    final TestBufferPool testBufferPool = makeTestBufferPool(closer);
    final TestGroupByBuffers groupByBuffers = makeGroupByBuffers(closer, processingConfig);

    return createQueryRunnerFactoryConglomerate(
        processingConfig,
        minTopNThreshold,
        jsonMapper,
        testBufferPool,
        groupByBuffers);
  }


  public static QueryRunnerFactoryConglomerate createQueryRunnerFactoryConglomerate(
      final RobuxProcessingConfig processingConfig,
      final Integer minTopNThreshold,
      final ObjectMapper jsonMapper,
      final TestBufferPool testBufferPool,
      final TestGroupByBuffers groupByBuffers)
  {
    ImmutableMap<Class<? extends Query>, QueryRunnerFactory> factories = makeDefaultQueryRunnerFactories(
        processingConfig,
        minTopNThreshold,
        jsonMapper,
        testBufferPool,
        groupByBuffers
    );
    UnionQueryLogic unionQueryLogic = new UnionQueryLogic();
    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        factories,
        Maps.transformValues(factories, f -> f.getToolchest()),
        ImmutableMap.of(UnionQuery.class, unionQueryLogic)
    );
    unionQueryLogic.initialize(conglomerate);

    return conglomerate;
  }

  @SuppressWarnings("rawtypes")
  public static ImmutableMap<Class<? extends Query>, QueryRunnerFactory> makeDefaultQueryRunnerFactories(
      final RobuxProcessingConfig processingConfig,
      final Integer minTopNThreshold,
      final ObjectMapper jsonMapper,
      final TestBufferPool testBufferPool,
      final TestGroupByBuffers groupByBuffers)
  {
    final GroupByQueryRunnerFactory groupByQueryRunnerFactory = GroupByQueryRunnerTest.makeQueryRunnerFactory(
        jsonMapper,
        new GroupByQueryConfig()
        {
        },
        groupByBuffers,
        processingConfig
    );

    return ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
        .put(
            SegmentMetadataQuery.class,
            new SegmentMetadataQueryRunnerFactory(
                new SegmentMetadataQueryQueryToolChest(
                    new SegmentMetadataQueryConfig("P1W")
                ),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
        .put(
            SearchQuery.class,
            new SearchQueryRunnerFactory(
                new SearchStrategySelector(Suppliers.ofInstance(new SearchQueryConfig())),
                new SearchQueryQueryToolChest(new SearchQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
        .put(
            ScanQuery.class,
            new ScanQueryRunnerFactory(
                new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                new ScanQueryEngine(),
                new ScanQueryConfig()
            )
        )
        .put(
            TimeseriesQuery.class,
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(),
                new TimeseriesQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
        .put(
            TopNQuery.class,
            new TopNQueryRunnerFactory(
                testBufferPool,
                new TopNQueryQueryToolChest(new TopNQueryConfig()
                {
                  @Override
                  public int getMinTopNThreshold()
                  {
                    return minTopNThreshold;
                  }
                }),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
        .put(GroupByQuery.class, groupByQueryRunnerFactory)
        .put(TimeBoundaryQuery.class, new TimeBoundaryQueryRunnerFactory(QueryRunnerTestHelper.NOOP_QUERYWATCHER))
        .put(
            WindowOperatorQuery.class,
            new WindowOperatorQueryQueryRunnerFactory(
                new WindowOperatorQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance())
            )
        )
        .build();
  }

  public static JoinableFactory makeJoinableFactoryForLookup(
      LookupExtractorFactoryContainerProvider lookupProvider
  )
  {
    return makeJoinableFactoryFromDefault(lookupProvider, null, null);
  }

  public static JoinableFactory makeJoinableFactoryFromDefault(
      @Nullable LookupExtractorFactoryContainerProvider lookupProvider,
      @Nullable Set<JoinableFactory> customFactories,
      @Nullable Map<Class<? extends JoinableFactory>, Class<? extends DataSource>> customMappings
  )
  {
    ImmutableSet.Builder<JoinableFactory> setBuilder = ImmutableSet.builder();
    ImmutableMap.Builder<Class<? extends JoinableFactory>, Class<? extends DataSource>> mapBuilder =
        ImmutableMap.builder();
    setBuilder.add(new InlineJoinableFactory(), new FrameBasedInlineJoinableFactory());
    mapBuilder.put(InlineJoinableFactory.class, InlineDataSource.class);
    mapBuilder.put(FrameBasedInlineJoinableFactory.class, FrameBasedInlineDataSource.class);
    if (lookupProvider != null) {
      setBuilder.add(new LookupJoinableFactory(lookupProvider));
      mapBuilder.put(LookupJoinableFactory.class, LookupDataSource.class);
    }
    if (customFactories != null) {
      setBuilder.addAll(customFactories);
    }
    if (customMappings != null) {
      mapBuilder.putAll(customMappings);
    }

    return new MapJoinableFactory(setBuilder.build(), mapBuilder.build());
  }

  public static RobuxInjectorBuilder defaultInjectorBuilder()
  {
    Injector startupInjector = new StartupInjectorBuilder()
        .build();

    RobuxInjectorBuilder injectorBuilder = new CoreInjectorBuilder(startupInjector)
        .ignoreLoadScopes()
        .addModule(new ExpressionModule())
        .addModule(new SegmentWranglerModule())
        .addModule(new CacheTestHelperModule(ResultCacheMode.DISABLED));

    return injectorBuilder;
  }

  public static Injector injectorWithLookup()
  {

    final LookupExtractorFactoryContainerProvider lookupProvider;
    lookupProvider = LookupEnabledTestExprMacroTable.createTestLookupProvider(Collections.emptyMap());

    return defaultInjectorBuilder()
        .addModule(binder -> binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupProvider))
        .build();
  }
}

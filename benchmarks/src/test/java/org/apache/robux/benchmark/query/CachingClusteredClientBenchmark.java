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

package org.apache.robux.benchmark.query;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.apache.robux.client.BrokerViewOfCoordinatorConfig;
import org.apache.robux.client.CachingClusteredClient;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.ImmutableRobuxServer;
import org.apache.robux.client.QueryableRobuxServer;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.client.cache.CachePopulatorStats;
import org.apache.robux.client.cache.ForegroundCachePopulator;
import org.apache.robux.client.cache.MapCache;
import org.apache.robux.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.robux.client.selector.RandomServerSelectorStrategy;
import org.apache.robux.client.selector.ServerSelector;
import org.apache.robux.client.selector.TierSelectorStrategy;
import org.apache.robux.collections.BlockingPool;
import org.apache.robux.collections.DefaultBlockingPool;
import org.apache.robux.collections.NonBlockingPool;
import org.apache.robux.collections.StupidPool;
import org.apache.robux.guice.http.RobuxHttpClientConfig;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.java.util.common.granularity.Granularity;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.BaseQuery;
import org.apache.robux.query.BrokerParallelMergeConfig;
import org.apache.robux.query.BySegmentQueryRunner;
import org.apache.robux.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.FinalizeResultsQueryRunner;
import org.apache.robux.query.FluentQueryRunner;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryContexts;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerFactory;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QueryRunnerTestHelper;
import org.apache.robux.query.Result;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.query.aggregation.LongSumAggregatorFactory;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.query.dimension.DefaultDimensionSpec;
import org.apache.robux.query.expression.TestExprMacroTable;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.query.groupby.GroupByQueryConfig;
import org.apache.robux.query.groupby.GroupByQueryQueryToolChest;
import org.apache.robux.query.groupby.GroupByQueryRunnerFactory;
import org.apache.robux.query.groupby.GroupByQueryRunnerTest;
import org.apache.robux.query.groupby.GroupByResourcesReservationPool;
import org.apache.robux.query.groupby.GroupByStatsProvider;
import org.apache.robux.query.groupby.GroupingEngine;
import org.apache.robux.query.groupby.ResultRow;
import org.apache.robux.query.spec.MultipleIntervalSegmentSpec;
import org.apache.robux.query.spec.QuerySegmentSpec;
import org.apache.robux.query.timeseries.TimeseriesQuery;
import org.apache.robux.query.timeseries.TimeseriesQueryEngine;
import org.apache.robux.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.robux.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.robux.query.timeseries.TimeseriesResultValue;
import org.apache.robux.query.topn.TopNQuery;
import org.apache.robux.query.topn.TopNQueryBuilder;
import org.apache.robux.query.topn.TopNQueryConfig;
import org.apache.robux.query.topn.TopNQueryQueryToolChest;
import org.apache.robux.query.topn.TopNQueryRunnerFactory;
import org.apache.robux.query.topn.TopNResultValue;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.QueryableIndexSegment;
import org.apache.robux.segment.generator.GeneratorBasicSchemas;
import org.apache.robux.segment.generator.GeneratorSchemaInfo;
import org.apache.robux.segment.generator.SegmentGenerator;
import org.apache.robux.server.ClientQuerySegmentWalker;
import org.apache.robux.server.QueryStackTests;
import org.apache.robux.server.ResourceIdPopulatingQueryRunner;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordination.TestCoordinatorClient;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.DataSegment.PruneSpecsHolder;
import org.apache.robux.timeline.SegmentId;
import org.apache.robux.timeline.TimelineLookup;
import org.apache.robux.timeline.VersionedIntervalTimeline;
import org.apache.robux.timeline.partition.LinearShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class CachingClusteredClientBenchmark
{
  private static final Logger LOG = new Logger(CachingClusteredClientBenchmark.class);
  private static final int PROCESSING_BUFFER_SIZE = 10 * 1024 * 1024; // ~10MiB
  private static final String DATA_SOURCE = "ds";

  public static final ObjectMapper JSON_MAPPER;

  @Param({"8", "24"})
  private int numServers;

  @Param({"0", "1", "4"})
  private int parallelism;

  @Param({"75000"})
  private int rowsPerSegment;

  @Param({"all", "minute"})
  private String queryGranularity;

  private QueryRunnerFactoryConglomerate conglomerate;
  private CachingClusteredClient cachingClusteredClient;
  private ExecutorService processingPool;
  private ForkJoinPool forkJoinPool;

  private boolean parallelCombine;

  private Query query;

  private final Closer closer = Closer.create();

  private final GeneratorSchemaInfo basicSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
  private final QuerySegmentSpec basicSchemaIntervalSpec = new MultipleIntervalSegmentSpec(
      Collections.singletonList(basicSchema.getDataInterval())
  );
  private final BrokerViewOfCoordinatorConfig filter = new BrokerViewOfCoordinatorConfig(new TestCoordinatorClient());

  private final int numProcessingThreads = 4;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(
        new Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), JSON_MAPPER)
            .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT)
    );
  }

  @Setup(Level.Trial)
  public void setup()
  {
    final String schemaName = "basic";

    parallelCombine = parallelism > 0;

    GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schemaName);

    Map<DataSegment, QueryableIndex> queryableIndexes = Maps.newHashMapWithExpectedSize(numServers);

    for (int i = 0; i < numServers; i++) {

      final DataSegment dataSegment = DataSegment.builder()
                                                 .dataSource(DATA_SOURCE)
                                                 .interval(schemaInfo.getDataInterval())
                                                 .version("1")
                                                 .shardSpec(new LinearShardSpec(i))
                                                 .size(0)
                                                 .build();
      final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
      LOG.info(
          "Starting benchmark setup using cacheDir[%s], rows[%,d].",
          segmentGenerator.getCacheDir(),
          rowsPerSegment
      );
      final QueryableIndex index = segmentGenerator.generate(
          dataSegment,
          schemaInfo,
          Granularities.NONE,
          rowsPerSegment
      );
      queryableIndexes.put(dataSegment, index);
      filter.start();
    }

    final RobuxProcessingConfig processingConfig = new RobuxProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return null;
      }

      @Override
      public int intermediateComputeSizeBytes()
      {
        return PROCESSING_BUFFER_SIZE;
      }

      @Override
      public int getNumMergeBuffers()
      {
        return 1;
      }

      @Override
      public int getNumThreads()
      {
        return numProcessingThreads;
      }
    };

    conglomerate = DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
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
                new StupidPool<>(
                    "TopNQueryRunnerFactory-bufferPool",
                    () -> ByteBuffer.allocate(PROCESSING_BUFFER_SIZE)
                ),
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
        .put(
            GroupByQuery.class,
            makeGroupByQueryRunnerFactory(
                GroupByQueryRunnerTest.DEFAULT_MAPPER,
                new GroupByQueryConfig()
                {
                },
                processingConfig
            )
        )
        .build());

    SimpleServerView serverView = new SimpleServerView();
    int serverSuffx = 1;
    for (Entry<DataSegment, QueryableIndex> entry : queryableIndexes.entrySet()) {
      serverView.addServer(
          createServer(serverSuffx++),
          entry.getKey(),
          entry.getValue()
      );
    }

    processingPool = Execs.multiThreaded(processingConfig.getNumThreads(), "caching-clustered-client-benchmark");
    forkJoinPool = new ForkJoinPool(
        (int) Math.ceil(Runtime.getRuntime().availableProcessors() * 0.75),
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null,
        true
    );
    cachingClusteredClient = new CachingClusteredClient(
        conglomerate,
        serverView,
        MapCache.create(0),
        JSON_MAPPER,
        new ForegroundCachePopulator(JSON_MAPPER, new CachePopulatorStats(), 0),
        new CacheConfig(),
        new RobuxHttpClientConfig(),
        new BrokerParallelMergeConfig() {
          @Override
          public boolean useParallelMergePool()
          {
            return true;
          }
        },
        forkJoinPool,
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new NoopServiceEmitter()
    );
  }

  private static GroupByQueryRunnerFactory makeGroupByQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final RobuxProcessingConfig processingConfig
  )
  {
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final Supplier<ByteBuffer> bufferSupplier =
        () -> ByteBuffer.allocateDirect(processingConfig.intermediateComputeSizeBytes());

    final NonBlockingPool<ByteBuffer> bufferPool = new StupidPool<>(
        "GroupByQueryEngine-bufferPool",
        bufferSupplier
    );
    final BlockingPool<ByteBuffer> mergeBufferPool = new DefaultBlockingPool<>(
        bufferSupplier,
        processingConfig.getNumMergeBuffers()
    );
    final GroupByStatsProvider groupByStatsProvider = new GroupByStatsProvider();
    final GroupByResourcesReservationPool groupByResourcesReservationPool =
        new GroupByResourcesReservationPool(mergeBufferPool, config);
    final GroupingEngine groupingEngine = new GroupingEngine(
        processingConfig,
        configSupplier,
        groupByResourcesReservationPool,
        mapper,
        mapper,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        groupByStatsProvider
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(groupingEngine, groupByResourcesReservationPool);
    return new GroupByQueryRunnerFactory(groupingEngine, toolChest, bufferPool);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException
  {
    closer.close();
    processingPool.shutdown();
    forkJoinPool.shutdownNow();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void timeseriesQuery(Blackhole blackhole)
  {
    Query<?> q = Robuxs.newTimeseriesQueryBuilder()
                       .dataSource(DATA_SOURCE)
                       .intervals(basicSchemaIntervalSpec)
                       .aggregators(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"))
                       .granularity(Granularity.fromString(queryGranularity))
                       .context(
                           ImmutableMap.of(
                               BaseQuery.QUERY_ID, "BenchmarkQuery",
                               QueryContexts.BROKER_PARALLEL_MERGE_KEY, parallelCombine,
                               QueryContexts.BROKER_PARALLELISM, parallelism
                           )
                       )
                       .build();

    query = prepareQuery(q);

    final List<Result<TimeseriesResultValue>> results = runQuery();

    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void topNQuery(Blackhole blackhole)
  {
    Query<?> q = new TopNQueryBuilder()
        .dataSource(DATA_SOURCE)
        .intervals(basicSchemaIntervalSpec)
        .dimension(new DefaultDimensionSpec("dimZipf", null))
        .aggregators(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"))
        .granularity(Granularity.fromString(queryGranularity))
        .metric("sumLongSequential")
        .threshold(10_000) // we are primarily measuring 'broker' merge time, so collect a significant number of results
        .context(
            ImmutableMap.of(
                BaseQuery.QUERY_ID, "BenchmarkQuery",
                QueryContexts.BROKER_PARALLEL_MERGE_KEY, parallelCombine,
                QueryContexts.BROKER_PARALLELISM, parallelism
            )
        )
        .build();

    query = prepareQuery(q);

    final List<Result<TopNResultValue>> results = runQuery();

    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void groupByQuery(Blackhole blackhole)
  {
    Query<?> q = GroupByQuery
        .builder()
        .setDataSource(DATA_SOURCE)
        .setQuerySegmentSpec(basicSchemaIntervalSpec)
        .setDimensions(
            new DefaultDimensionSpec("dimZipf", null),
            new DefaultDimensionSpec("dimSequential", null)
        )
        .setAggregatorSpecs(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"))
        .setGranularity(Granularity.fromString(queryGranularity))
        .setContext(
            ImmutableMap.of(
                BaseQuery.QUERY_ID, "BenchmarkQuery",
                QueryContexts.BROKER_PARALLEL_MERGE_KEY, parallelCombine,
                QueryContexts.BROKER_PARALLELISM, parallelism
            )
        )
        .build();

    query = prepareQuery(q);

    final List<ResultRow> results = runQuery();

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  private <T> Query<T> prepareQuery(Query<T> query)
  {
    return ResourceIdPopulatingQueryRunner.populateResourceId(query)
                                          .withDataSource(ClientQuerySegmentWalker.generateSubqueryIds(
                                              query.getDataSource(),
                                              query.getId(),
                                              query.getSqlQueryId(),
                                              query.context().getString(QueryContexts.QUERY_RESOURCE_ID)
                                          ));
  }

  private <T> List<T> runQuery()
  {
    //noinspection unchecked
    QueryRunner<T> theRunner = FluentQueryRunner
        .create(
            cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals()),
            conglomerate.getToolChest(query)
        )
        .applyPreMergeDecoration()
        .mergeResults(true)
        .applyPostMergeDecoration();

    //noinspection unchecked
    Sequence<T> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());

    return queryResult.toList();
  }

  private class SimpleServerView implements TimelineServerView
  {
    private final TierSelectorStrategy tierSelectorStrategy = new HighestPriorityTierSelectorStrategy(
        new RandomServerSelectorStrategy()
    );
    // server -> queryRunner
    private final Map<RobuxServer, SingleSegmentRobuxServer> servers = new HashMap<>();
    // segmentId -> serverSelector
    private final Map<String, ServerSelector> selectors = new HashMap<>();
    // dataSource -> version -> serverSelector
    private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines = new HashMap<>();

    void addServer(RobuxServer server, DataSegment dataSegment, QueryableIndex queryableIndex)
    {
      servers.put(
          server,
          new SingleSegmentRobuxServer(
              server,
              new SimpleQueryRunner(
                  conglomerate,
                  dataSegment.getId(),
                  queryableIndex
              )
          )
      );
      addSegmentToServer(server, dataSegment);
    }

    void addSegmentToServer(RobuxServer server, DataSegment segment)
    {
      final ServerSelector selector = selectors.computeIfAbsent(
          segment.getId().toString(),
          k -> new ServerSelector(segment, tierSelectorStrategy, filter)
      );
      selector.addServerAndUpdateSegment(servers.get(server), segment);
      timelines.computeIfAbsent(segment.getDataSource(), k -> new VersionedIntervalTimeline<>(Ordering.natural()))
               .add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
    }

    @Override
    public Optional<? extends TimelineLookup<String, ServerSelector>> getTimeline(TableDataSource table)
    {
      return Optional.ofNullable(timelines.get(table.getName()));
    }

    @Override
    public List<ImmutableRobuxServer> getRobuxServers()
    {
      return Collections.emptyList();
    }

    @Override
    public <T> QueryRunner<T> getQueryRunner(RobuxServer server)
    {
      final SingleSegmentRobuxServer queryableRobuxServer = Preconditions.checkNotNull(servers.get(server), "server");
      return (QueryRunner<T>) queryableRobuxServer.getQueryRunner();
    }

    @Override
    public void registerTimelineCallback(Executor exec, TimelineCallback callback)
    {
      // do nothing
    }

    @Override
    public void registerServerCallback(Executor exec, ServerCallback callback)
    {
      // do nothing
    }

    @Override
    public void registerSegmentCallback(Executor exec, SegmentCallback callback)
    {
      // do nothing
    }
  }

  private static class SimpleQueryRunner implements QueryRunner<Object>
  {
    private final QueryRunnerFactoryConglomerate conglomerate;
    private final QueryableIndexSegment segment;

    public SimpleQueryRunner(
        QueryRunnerFactoryConglomerate conglomerate,
        SegmentId segmentId,
        QueryableIndex queryableIndex
    )
    {
      this.conglomerate = conglomerate;
      this.segment = new QueryableIndexSegment(queryableIndex, segmentId);
    }

    @Override
    public Sequence<Object> run(QueryPlus<Object> queryPlus, ResponseContext responseContext)
    {
      final QueryRunnerFactory factory = conglomerate.findFactory(queryPlus.getQuery());
      //noinspection unchecked
      return factory.getToolchest().preMergeQueryDecoration(
          new FinalizeResultsQueryRunner<>(
              new BySegmentQueryRunner<>(
                  segment.getId(),
                  segment.getDataInterval().getStart(),
                  factory.createRunner(segment)
              ),
              factory.getToolchest()
          )
      ).run(queryPlus, responseContext);
    }
  }

  private static class SingleSegmentRobuxServer extends QueryableRobuxServer
  {
    SingleSegmentRobuxServer(RobuxServer server, SimpleQueryRunner runner)
    {
      super(server, runner);
    }
  }

  private static RobuxServer createServer(int nameSuiffix)
  {
    return new RobuxServer(
        "server_" + nameSuiffix,
        "127.0.0." + nameSuiffix,
        null,
        Long.MAX_VALUE,
        ServerType.HISTORICAL,
        "default",
        0
    );
  }
}

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

package org.apache.robux.benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.benchmark.query.QueryBenchmarkUtil;
import org.apache.robux.data.input.InputRow;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.FileUtils;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.FinalizeResultsQueryRunner;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryContexts;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerFactory;
import org.apache.robux.query.QueryToolChest;
import org.apache.robux.query.Result;
import org.apache.robux.query.aggregation.AggregatorFactory;
import org.apache.robux.query.aggregation.CountAggregatorFactory;
import org.apache.robux.query.aggregation.FilteredAggregatorFactory;
import org.apache.robux.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.query.filter.BoundDimFilter;
import org.apache.robux.query.filter.DimFilter;
import org.apache.robux.query.filter.InDimFilter;
import org.apache.robux.query.filter.OrDimFilter;
import org.apache.robux.query.filter.RegexDimFilter;
import org.apache.robux.query.filter.SearchQueryDimFilter;
import org.apache.robux.query.ordering.StringComparators;
import org.apache.robux.query.search.ContainsSearchQuerySpec;
import org.apache.robux.query.spec.MultipleIntervalSegmentSpec;
import org.apache.robux.query.spec.QuerySegmentSpec;
import org.apache.robux.query.timeseries.TimeseriesQuery;
import org.apache.robux.query.timeseries.TimeseriesQueryEngine;
import org.apache.robux.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.robux.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.robux.query.timeseries.TimeseriesResultValue;
import org.apache.robux.segment.IncrementalIndexSegment;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMergerV9;
import org.apache.robux.segment.IndexSpec;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.QueryableIndexSegment;
import org.apache.robux.segment.column.ColumnConfig;
import org.apache.robux.segment.generator.DataGenerator;
import org.apache.robux.segment.generator.GeneratorBasicSchemas;
import org.apache.robux.segment.generator.GeneratorSchemaInfo;
import org.apache.robux.segment.incremental.AppendableIndexSpec;
import org.apache.robux.segment.incremental.IncrementalIndex;
import org.apache.robux.segment.incremental.IncrementalIndexCreator;
import org.apache.robux.segment.incremental.OnheapIncrementalIndex;
import org.apache.robux.segment.serde.ComplexMetrics;
import org.apache.robux.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.robux.timeline.SegmentId;
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class FilteredAggregatorBenchmark
{
  @Param({"75000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"false", "true"})
  private String vectorize;

  @Param({"true", "false"})
  private boolean descending;

  private static final Logger log = new Logger(FilteredAggregatorBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private AppendableIndexSpec appendableIndexSpec;
  private AggregatorFactory filteredMetric;
  private DimFilter filter;
  private DataGenerator generator;
  private QueryRunnerFactory factory;
  private GeneratorSchemaInfo schemaInfo;
  private TimeseriesQuery query;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  /**
   * Setup everything common for benchmarking both the incremental-index and the queriable-index.
   */
  @Setup
  public void setup()
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schema);

    generator = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

    filter = new OrDimFilter(
        Arrays.asList(
            new BoundDimFilter("dimSequential", "-1", "-1", true, true, null, null, StringComparators.ALPHANUMERIC),
            new RegexDimFilter("dimSequential", "X", null),
            new SearchQueryDimFilter("dimSequential", new ContainsSearchQuerySpec("X", false), null),
            new InDimFilter("dimSequential", Collections.singletonList("X"), null)
        )
    );
    filteredMetric = new FilteredAggregatorFactory(new CountAggregatorFactory("rows"), filter);

    factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        QueryBenchmarkUtil.NOOP_QUERYWATCHER
    );

    GeneratorSchemaInfo basicSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));
    List<AggregatorFactory> queryAggs = Collections.singletonList(filteredMetric);

    query = Robuxs.newTimeseriesQueryBuilder()
                  .dataSource("blah")
                  .granularity(Granularities.ALL)
                  .intervals(intervalSpec)
                  .aggregators(queryAggs)
                  .descending(descending)
                  .build();
  }

  /**
   * Setup/teardown everything specific for benchmarking the incremental-index.
   */
  @State(Scope.Benchmark)
  public static class IncrementalIndexState
  {
    @Param({"onheap", "offheap"})
    private String indexType;

    IncrementalIndex incIndex;

    @Setup
    public void setup(FilteredAggregatorBenchmark global) throws JsonProcessingException
    {
      // Creates an AppendableIndexSpec that corresponds to the indexType parametrization.
      // It is used in {@code global.makeIncIndex()} to instanciate an incremental-index of the specified type.
      global.appendableIndexSpec = IncrementalIndexCreator.parseIndexType(indexType);
      incIndex = global.makeIncIndex(global.schemaInfo.getAggsArray());
      global.generator.addToIndex(incIndex, global.rowsPerSegment);
    }

    @TearDown
    public void tearDown()
    {
      if (incIndex != null) {
        incIndex.close();
      }
    }
  }

  /**
   * Setup/teardown everything specific for benchmarking the ingestion of the incremental-index.
   */
  @State(Scope.Benchmark)
  public static class IncrementalIndexIngestState
  {
    @Param({"onheap", "offheap"})
    private String indexType;

    IncrementalIndex incIndex;
    List<InputRow> inputRows;

    @Setup(Level.Invocation)
    public void setup(FilteredAggregatorBenchmark global) throws JsonProcessingException
    {
      // Creates an AppendableIndexSpec that corresponds to the indexType parametrization.
      // It is used in {@code global.makeIncIndex()} to instanciate an incremental-index of the specified type.
      global.appendableIndexSpec = IncrementalIndexCreator.parseIndexType(indexType);
      inputRows = global.generator.toList(global.rowsPerSegment);
      incIndex = global.makeIncIndex(new AggregatorFactory[]{global.filteredMetric});
    }

    @TearDown(Level.Invocation)
    public void tearDown()
    {
      if (incIndex != null) {
        incIndex.close();
      }
    }
  }

  /**
   * Setup/teardown everything specific for benchmarking the queriable-index.
   */
  @State(Scope.Benchmark)
  public static class QueryableIndexState
  {
    private File qIndexesDir;
    private QueryableIndex qIndex;

    @Setup
    public void setup(FilteredAggregatorBenchmark global) throws IOException
    {
      global.appendableIndexSpec = new OnheapIncrementalIndex.Spec();

      IncrementalIndex incIndex = global.makeIncIndex(global.schemaInfo.getAggsArray());
      global.generator.addToIndex(incIndex, global.rowsPerSegment);

      qIndexesDir = FileUtils.createTempDir();
      log.info("Using temp dir: " + qIndexesDir.getAbsolutePath());

      File indexFile = INDEX_MERGER_V9.persist(
          incIndex,
          qIndexesDir,
          IndexSpec.DEFAULT,
          null
      );
      incIndex.close();

      qIndex = INDEX_IO.loadIndex(indexFile);
    }

    @TearDown
    public void tearDown()
    {
      if (qIndex != null) {
        qIndex.close();
      }
      if (qIndexesDir != null) {
        qIndexesDir.delete();
      }
    }
  }

  private IncrementalIndex makeIncIndex(AggregatorFactory[] metrics)
  {
    return appendableIndexSpec.builder()
        .setSimpleTestingIndexSchema(metrics)
        .setMaxRowCount(rowsPerSegment)
        .build();
  }

  private static <T> List<T> runQuery(QueryRunnerFactory factory, QueryRunner runner, Query<T> query, String vectorize)
  {
    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    final QueryPlus<T> queryToRun = QueryPlus.wrap(
        query.withOverriddenContext(
            ImmutableMap.of(
                QueryContexts.VECTORIZE_KEY, vectorize,
                QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
            )
        )
    );
    Sequence<T> queryResult = theRunner.run(queryToRun, ResponseContext.createEmpty());
    return queryResult.toList();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void ingest(Blackhole blackhole, IncrementalIndexIngestState state) throws Exception
  {
    for (InputRow row : state.inputRows) {
      int rv = state.incIndex.add(row).getRowCount();
      blackhole.consume(rv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleIncrementalIndex(Blackhole blackhole, IncrementalIndexState state)
  {
    QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("incIndex"),
        new IncrementalIndexSegment(state.incIndex, SegmentId.dummy("incIndex"))
    );

    List<Result<TimeseriesResultValue>> results = FilteredAggregatorBenchmark.runQuery(
        factory,
        runner,
        query,
        vectorize
    );
    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndex(Blackhole blackhole, QueryableIndexState state)
  {
    final QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("qIndex"),
        new QueryableIndexSegment(state.qIndex, SegmentId.dummy("qIndex"))
    );

    List<Result<TimeseriesResultValue>> results = FilteredAggregatorBenchmark.runQuery(
        factory,
        runner,
        query,
        vectorize
    );
    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }
}

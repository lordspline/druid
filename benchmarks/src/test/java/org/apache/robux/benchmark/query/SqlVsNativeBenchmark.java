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

import com.google.common.collect.ImmutableSet;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.aggregation.CountAggregatorFactory;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.query.dimension.DefaultDimensionSpec;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.query.groupby.ResultRow;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.generator.GeneratorBasicSchemas;
import org.apache.robux.segment.generator.GeneratorSchemaInfo;
import org.apache.robux.segment.generator.SegmentGenerator;
import org.apache.robux.server.QueryStackTests;
import org.apache.robux.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.robux.server.security.AuthConfig;
import org.apache.robux.server.security.AuthTestUtils;
import org.apache.robux.sql.calcite.planner.CalciteRulesManager;
import org.apache.robux.sql.calcite.planner.CatalogResolver;
import org.apache.robux.sql.calcite.planner.RobuxPlanner;
import org.apache.robux.sql.calcite.planner.PlannerConfig;
import org.apache.robux.sql.calcite.planner.PlannerFactory;
import org.apache.robux.sql.calcite.planner.PlannerResult;
import org.apache.robux.sql.calcite.run.SqlEngine;
import org.apache.robux.sql.calcite.schema.RobuxSchemaCatalog;
import org.apache.robux.sql.calcite.util.CalciteTests;
import org.apache.robux.sql.hook.RobuxHookDispatcher;
import org.apache.robux.timeline.DataSegment;
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

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that compares the same groupBy query through the native query layer and through the SQL layer.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 15)
@Measurement(iterations = 30)
public class SqlVsNativeBenchmark
{
  @Param({"200000", "1000000"})
  private int rowsPerSegment;

  private static final Logger log = new Logger(SqlVsNativeBenchmark.class);

  private SpecificSegmentsQuerySegmentWalker walker;
  private SqlEngine engine;
  private PlannerFactory plannerFactory;
  private GroupByQuery groupByQuery;
  private String sqlQuery;
  private Closer closer;

  @Setup(Level.Trial)
  public void setup()
  {
    this.closer = Closer.create();

    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    log.info("Starting benchmark setup using tmpDir[%s], rows[%,d].", segmentGenerator.getCacheDir(), rowsPerSegment);

    final QueryableIndex index = segmentGenerator.generate(dataSegment, schemaInfo, Granularities.NONE, rowsPerSegment);
    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
    final PlannerConfig plannerConfig = new PlannerConfig();

    this.walker = closer.register(SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate).add(dataSegment, index));
    final RobuxSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    engine = CalciteTests.createMockSqlEngine(walker, conglomerate);
    plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.ROBUX_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.createJoinableFactoryWrapper(),
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        new RobuxHookDispatcher()
    );
    groupByQuery = GroupByQuery
        .builder()
        .setDataSource("foo")
        .setInterval(Intervals.ETERNITY)
        .setDimensions(new DefaultDimensionSpec("dimZipf", "d0"), new DefaultDimensionSpec("dimSequential", "d1"))
        .setAggregatorSpecs(new CountAggregatorFactory("c"))
        .setGranularity(Granularities.ALL)
        .build();

    sqlQuery = "SELECT\n"
               + "  dimZipf AS d0,"
               + "  dimSequential AS d1,\n"
               + "  COUNT(*) AS c\n"
               + "FROM robux.foo\n"
               + "GROUP BY dimZipf, dimSequential";
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryNative(Blackhole blackhole)
  {
    final Sequence<ResultRow> resultSequence = QueryPlus.wrap(groupByQuery).run(walker, ResponseContext.createEmpty());
    final ResultRow lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
    blackhole.consume(lastRow);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryPlanner(Blackhole blackhole)
  {
    try (final RobuxPlanner planner = plannerFactory.createPlannerForTesting(engine, sqlQuery, Collections.emptyMap())) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }
}

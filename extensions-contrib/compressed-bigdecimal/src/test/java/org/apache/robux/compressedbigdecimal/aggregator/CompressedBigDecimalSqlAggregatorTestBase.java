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

package org.apache.robux.compressedbigdecimal.aggregator;

import com.google.common.collect.ImmutableList;
import org.apache.robux.compressedbigdecimal.CompressedBigDecimalModule;
import org.apache.robux.compressedbigdecimal.aggregator.CompressedBigDecimalSqlAggregatorTestBase.CompressedBigDecimalComponentSupplier;
import org.apache.robux.data.input.InputRow;
import org.apache.robux.data.input.InputRowSchema;
import org.apache.robux.data.input.impl.DimensionsSpec;
import org.apache.robux.data.input.impl.TimestampSpec;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.aggregation.CountAggregatorFactory;
import org.apache.robux.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.robux.query.spec.MultipleIntervalSegmentSpec;
import org.apache.robux.segment.IndexBuilder;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.incremental.IncrementalIndexSchema;
import org.apache.robux.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.robux.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.robux.sql.calcite.BaseCalciteQueryTest;
import org.apache.robux.sql.calcite.SqlTestFrameworkConfig;
import org.apache.robux.sql.calcite.TempDirProducer;
import org.apache.robux.sql.calcite.filtration.Filtration;
import org.apache.robux.sql.calcite.util.CalciteTests;
import org.apache.robux.sql.calcite.util.RobuxModuleCollection;
import org.apache.robux.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.robux.sql.calcite.util.TestDataBuilder;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.LinearShardSpec;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@SqlTestFrameworkConfig.ComponentSupplier(CompressedBigDecimalComponentSupplier.class)
public abstract class CompressedBigDecimalSqlAggregatorTestBase extends BaseCalciteQueryTest
{
  protected static class CompressedBigDecimalComponentSupplier extends StandardComponentSupplier
  {
    public CompressedBigDecimalComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    private static final InputRowSchema SCHEMA = new InputRowSchema(
        new TimestampSpec(TestDataBuilder.TIMESTAMP_COLUMN, "iso", null),
        new DimensionsSpec(
            DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3", "m2"))
        ),
        null
    );

    private static final List<InputRow> ROWS1 =
        TestDataBuilder.RAW_ROWS1.stream().map(m -> TestDataBuilder.createRow(m, SCHEMA)).collect(Collectors.toList());

    @Override
    public RobuxModule getCoreModule()
    {
      return RobuxModuleCollection.of(super.getCoreModule(), new CompressedBigDecimalModule());
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      QueryableIndex index =
          IndexBuilder.create()
                      .tmpDir(tempDirProducer.newTempFolder())
                      .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                      .schema(
                          new IncrementalIndexSchema.Builder()
                              .withMetrics(
                                  new CountAggregatorFactory("cnt"),
                                  new DoubleSumAggregatorFactory("m1", "m1")
                              )
                              .withRollup(false)
                              .build()
                      )
                      .rows(ROWS1)
                      .buildMMappedIndex();

      return walker.add(
          DataSegment.builder()
                     .dataSource(CalciteTests.DATASOURCE1)
                     .interval(index.getDataInterval())
                     .version("1")
                     .shardSpec(new LinearShardSpec(0))
                     .size(0)
                     .build(),
          index
      );
    }
  }

  @Test
  public abstract void testCompressedBigDecimalAggWithNumberParse();

  // expected: NumberFormatException.class
  @Test
  public abstract void testCompressedBigDecimalAggWithStrictNumberParse();

  @Test
  public abstract void testCompressedBigDecimalAggDefaultNumberParseAndCustomSizeAndScale();

  @Test
  public abstract void testCompressedBigDecimalAggDefaultScale();

  @Test
  public abstract void testCompressedBigDecimalAggDefaultSizeAndScale();

  protected void testCompressedBigDecimalAggWithNumberParseHelper(
      String functionName,
      Object[] expectedResults,
      CompressedBigDecimalAggregatorFactoryCreator factoryCreator
  )
  {
    cannotVectorize();
    testQuery(
        StringUtils.format(
            "SELECT %s(m1, 9, 9), %s(m2, 9, 9), %s(dim1, 9, 9, false) FROM foo",
            functionName,
            functionName,
            functionName
        ),
        Collections.singletonList(
            Robuxs.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      factoryCreator.create("a0", "m1", 9, 9, false),
                      factoryCreator.create("a1", "m2", 9, 9, false),
                      factoryCreator.create("a2", "dim1", 9, 9, false)

                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(expectedResults)
    );
  }

  protected void testCompressedBigDecimalAggWithStrictNumberParseHelper(
      String functionName,
      CompressedBigDecimalAggregatorFactoryCreator factoryCreator
  )
  {
    cannotVectorize();
    testQuery(
        StringUtils.format("SELECT %s(dim1, 9, 9, true) FROM foo", functionName),
        Collections.singletonList(
            Robuxs.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(factoryCreator.create("a0", "dim1", 9, 9, true))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{"unused"})
    );
  }

  public void testCompressedBigDecimalAggDefaultNumberParseAndCustomSizeAndScaleHelper(
      String functionName,
      Object[] expectedResults,
      CompressedBigDecimalAggregatorFactoryCreator factoryCreator
  )
  {
    cannotVectorize();
    testQuery(
        StringUtils.format(
            "SELECT %s(m1, 9, 3), %s(m2, 9, 3), %s(dim1, 9, 3) FROM foo",
            functionName,
            functionName,
            functionName
        ),
        Collections.singletonList(
            Robuxs.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      factoryCreator.create("a0", "m1", 9, 3, false),
                      factoryCreator.create("a1", "m2", 9, 3, false),
                      factoryCreator.create("a2", "dim1", 9, 3, false)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(expectedResults)
    );
  }

  public void testCompressedBigDecimalAggDefaultScaleHelper(
      String functionName,
      Object[] expectedResults,
      CompressedBigDecimalAggregatorFactoryCreator factoryCreator
  )
  {
    cannotVectorize();
    testQuery(
        StringUtils.format(
            "SELECT %s(m1, 9), %s(m2, 9), %s(dim1, 9) FROM foo",
            functionName,
            functionName,
            functionName
        ),
        Collections.singletonList(
            Robuxs.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      factoryCreator.create("a0", "m1", 9, 9, false),
                      factoryCreator.create("a1", "m2", 9, 9, false),
                      factoryCreator.create("a2", "dim1", 9, 9, false)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(expectedResults)
    );
  }

  public void testCompressedBigDecimalAggDefaultSizeAndScaleHelper(
      String functionName,
      Object[] expectedResults,
      CompressedBigDecimalAggregatorFactoryCreator factoryCreator
  )
  {
    cannotVectorize();
    testQuery(
        StringUtils.format("SELECT %s(m1), %s(m2), %s(dim1) FROM foo", functionName, functionName, functionName),
        Collections.singletonList(
            Robuxs.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      factoryCreator.create("a0", "m1", 6, 9, false),
                      factoryCreator.create("a1", "m2", 6, 9, false),
                      factoryCreator.create("a2", "dim1", 6, 9, false)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(expectedResults)
    );
  }
}

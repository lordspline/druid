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

package org.apache.robux.msq.exec;

import com.google.common.collect.ImmutableList;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.msq.indexing.LegacyMSQSpec;
import org.apache.robux.msq.indexing.MSQTuningConfig;
import org.apache.robux.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.robux.msq.test.MSQTestBase;
import org.apache.robux.query.aggregation.FilteredAggregatorFactory;
import org.apache.robux.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.robux.query.dimension.DefaultDimensionSpec;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.filtration.Filtration;
import org.apache.robux.sql.calcite.planner.ColumnMapping;
import org.apache.robux.sql.calcite.planner.ColumnMappings;
import org.apache.robux.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Test;

/**
 * Tests of MSQ with functions from the "robux-datasketches" extension.
 */
public class MSQDataSketchesTest extends MSQTestBase
{
  @Test
  public void testHavingOnDsHll()
  {
    RowSignature resultSignature =
        RowSignature.builder()
                    .add("dim2", ColumnType.STRING)
                    .add("col", ColumnType.ofComplex("HLLSketchBuild"))
                    .build();

    GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                    .setAggregatorSpecs(
                        aggregators(
                            new HllSketchBuildAggregatorFactory("a0", "m1", 12, "HLL_4", null, false, true)
                        )
                    )
                    .setHavingSpec(having(expressionFilter(("(hll_sketch_estimate(\"a0\") > 1)"))))
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("SELECT dim2, DS_HLL(m1) as col\n"
                + "FROM foo\n"
                + "GROUP BY dim2\n"
                + "HAVING HLL_SKETCH_ESTIMATE(col) > 1")
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
                                   .query(query)
                                   .columnMappings(new ColumnMappings(ImmutableList.of(
                                       new ColumnMapping("d0", "dim2"),
                                       new ColumnMapping("a0", "col")
                                   )))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{null, "\"AgEHDAMIAgCOlN8Fp9xhBA==\""},
                new Object[]{"a", "\"AgEHDAMIAgALpZ0PPgu1BA==\""}
            )
        )
        .verifyResults();
  }

  @Test
  public void testEmptyHllSketch()
  {
    RowSignature resultSignature =
        RowSignature.builder()
                    .add("c", ColumnType.LONG)
                    .build();

    GroupByQuery query =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        aggregators(
                            new FilteredAggregatorFactory(
                                new HllSketchBuildAggregatorFactory("a0", "dim2", 12, "HLL_4", null, true, true),
                                equality("dim1", "nonexistent", ColumnType.STRING),
                                "a0"
                            )
                        )
                    )
                    .setContext(DEFAULT_MSQ_CONTEXT)
                    .build();

    testSelectQuery()
        .setSql("SELECT APPROX_COUNT_DISTINCT_DS_HLL(dim2) FILTER(WHERE dim1 = 'nonexistent') AS c FROM robux.foo")
        .setExpectedMSQSpec(LegacyMSQSpec.builder()
                                   .query(query)
                                   .columnMappings(new ColumnMappings(ImmutableList.of(
                                       new ColumnMapping("a0", "c"))
                                   ))
                                   .tuningConfig(MSQTuningConfig.defaultConfig())
                                   .destination(TaskReportMSQDestination.INSTANCE)
                                   .build())
        .setQueryContext(DEFAULT_MSQ_CONTEXT)
        .setExpectedRowSignature(resultSignature)
        .setExpectedResultRows(
            ImmutableList.of(
                new Object[]{0L}
            )
        )
        .verifyResults();
  }
}

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

import com.google.common.collect.ImmutableList;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.msq.sql.MSQTaskSqlEngine;
import org.apache.robux.query.QueryDataSource;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.query.UnionDataSource;
import org.apache.robux.query.aggregation.CountAggregatorFactory;
import org.apache.robux.query.aggregation.LongSumAggregatorFactory;
import org.apache.robux.query.dimension.DefaultDimensionSpec;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.sql.calcite.BaseCalciteQueryTest;
import org.apache.robux.sql.calcite.CalciteUnionQueryTest;
import org.apache.robux.sql.calcite.QueryTestBuilder;
import org.apache.robux.sql.calcite.SqlTestFrameworkConfig;
import org.apache.robux.sql.calcite.filtration.Filtration;
import org.apache.robux.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Runs {@link CalciteUnionQueryTest} but with MSQ engine
 */
@SqlTestFrameworkConfig.ComponentSupplier(StandardMSQComponentSupplier.class)
public class CalciteUnionQueryMSQTest extends CalciteUnionQueryTest
{
  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new BaseCalciteQueryTest.CalciteTestConfig(true))
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }

  /**
   * Generates a different error hint than what is required by the native engine, since planner does try to plan "UNION"
   * using group by, however fails due to the column name mismatch.
   * MSQ does wnat to support any type of data source, with least restrictive column names and types, therefore it
   * should eventually work.
   */
  @Test
  @Override
  public void testUnionDifferentColumnOrder()
  {
    assertQueryIsUnplannable(
        "SELECT dim2, dim1, m1 FROM foo2 UNION SELECT dim1, dim2, m1 FROM foo",
        "SQL requires union between two tables and column names queried for each table are different Left: [dim2, dim1, m1], Right: [dim1, dim2, m1]."
    );
  }

  @Disabled("Ignored till MSQ can plan UNION ALL with any operand")
  @Test
  public void testUnionOnSubqueries()
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (\n"
        + "  (SELECT dim2, SUM(cnt) AS cnt FROM robux.foo GROUP BY dim2)\n"
        + "  UNION ALL\n"
        + "  (SELECT dim2, SUM(cnt) AS cnt FROM robux.foo GROUP BY dim2)\n"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(
                                                new UnionDataSource(
                                                    ImmutableList.of(
                                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                                        new TableDataSource(CalciteTests.DATASOURCE1)
                                                    )
                                                )
                                            )
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new CountAggregatorFactory("_a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{12L, 4L}
        )
    );
  }

}

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

package org.apache.robux.query.timeseries;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.guava.Sequences;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerTestHelper;
import org.apache.robux.query.QueryToolChest;
import org.apache.robux.query.Result;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.query.UnionDataSource;
import org.apache.robux.query.UnionDataSourceQueryRunner;
import org.apache.robux.query.aggregation.LongSumAggregatorFactory;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class TimeSeriesUnionQueryRunnerTest extends InitializedNullHandlingTest
{
  private final QueryRunner runner;
  private final boolean descending;

  public TimeSeriesUnionQueryRunnerTest(QueryRunner runner, boolean descending)
  {
    this.runner = runner;
    this.descending = descending;
  }

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(
        QueryRunnerTestHelper.makeQueryRunnersToMerge(
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(),
                new TimeseriesQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            ),
            false
        ),
        // descending?
        Arrays.asList(false, true)
    );
  }

  private <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results)
  {
    if (descending) {
      expectedResults = TestHelper.revert(expectedResults);
    }
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testUnionTimeseries()
  {
    TimeseriesQuery query = Robuxs.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.UNION_DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          ),
                                          QueryRunnerTestHelper.QUALITY_UNIQUES
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 52L, "idx", 26476L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 52L, "idx", 23308L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );
    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testUnionResultMerging()
  {
    TimeseriesQuery query = Robuxs.newTimeseriesQueryBuilder()
                                  .dataSource(
                                      new UnionDataSource(
                                          Lists.newArrayList(
                                              new TableDataSource("ds1"),
                                              new TableDataSource("ds2")
                                          )
                                      )
                                  )
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .descending(descending)
                                  .build();
    QueryToolChest toolChest = new TimeseriesQueryQueryToolChest();
    final List<Result<TimeseriesResultValue>> ds1 = Lists.newArrayList(
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(ImmutableMap.of("rows", 1L, "idx", 2L))
        ),
        new Result<>(
            DateTimes.of("2011-04-03"),
            new TimeseriesResultValue(ImmutableMap.of("rows", 3L, "idx", 4L))
        )
    );
    final List<Result<TimeseriesResultValue>> ds2 = Lists.newArrayList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(ImmutableMap.of("rows", 5L, "idx", 6L))
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(ImmutableMap.of("rows", 7L, "idx", 8L))
        ),
        new Result<>(
            DateTimes.of("2011-04-04"),
            new TimeseriesResultValue(ImmutableMap.of("rows", 9L, "idx", 10L))
        )
    );

    QueryRunner mergingrunner = toolChest.mergeResults(
        new UnionDataSourceQueryRunner<>(
            new QueryRunner<Result<TimeseriesResultValue>>()
            {
              @Override
              public Sequence<Result<TimeseriesResultValue>> run(
                  QueryPlus<Result<TimeseriesResultValue>> queryPlus,
                  ResponseContext responseContext
              )
              {
                if (queryPlus.getQuery().getDataSource().equals(new TableDataSource("ds1"))) {
                  return Sequences.simple(descending ? Lists.reverse(ds1) : ds1);
                } else {
                  return Sequences.simple(descending ? Lists.reverse(ds2) : ds2);
                }
              }
            }
        )
    );

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 5L, "idx", 6L)
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 8L, "idx", 10L)
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-03"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 3L, "idx", 4L)
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-04"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 9L, "idx", 10L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = mergingrunner.run(QueryPlus.wrap(query)).toList();

    assertExpectedResults(expectedResults, results);

  }

}

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

package org.apache.robux.query.aggregation.distinctcount;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.robux.data.input.MapBasedInputRow;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.query.FluentQueryRunner;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunnerTestHelper;
import org.apache.robux.query.aggregation.CountAggregatorFactory;
import org.apache.robux.query.dimension.DefaultDimensionSpec;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.query.groupby.GroupByQueryConfig;
import org.apache.robux.query.groupby.GroupByQueryRunnerFactory;
import org.apache.robux.query.groupby.GroupByQueryRunnerTest;
import org.apache.robux.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.robux.query.groupby.ResultRow;
import org.apache.robux.query.groupby.TestGroupByBuffers;
import org.apache.robux.query.groupby.orderby.DefaultLimitSpec;
import org.apache.robux.query.groupby.orderby.OrderByColumnSpec;
import org.apache.robux.segment.IncrementalIndexSegment;
import org.apache.robux.segment.Segment;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.incremental.IncrementalIndex;
import org.apache.robux.segment.incremental.IncrementalIndexSchema;
import org.apache.robux.segment.incremental.OnheapIncrementalIndex;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DistinctCountGroupByQueryTest extends InitializedNullHandlingTest
{
  private GroupByQueryRunnerFactory factory;
  private Closer resourceCloser;

  @Before
  public void setup()
  {
    final GroupByQueryConfig config = new GroupByQueryConfig();
    this.resourceCloser = Closer.create();
    this.factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(
        config,
        this.resourceCloser.register(TestGroupByBuffers.createDefault())
    );
  }

  @After
  public void teardown() throws IOException
  {
    resourceCloser.close();
  }

  @Test
  public void testGroupByWithDistinctCountAgg() throws Exception
  {
    IncrementalIndex index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withQueryGranularity(Granularities.SECOND)
                .withMetrics(new CountAggregatorFactory("cnt"))
                .build()
        )
        .setMaxRowCount(1000)
        .build();

    String visitor_id = "visitor_id";
    String client_type = "client_type";
    long timestamp = DateTimes.of("2010-01-01").getMillis();
    index.add(
        new MapBasedInputRow(
            timestamp,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.of(visitor_id, "0", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp + 1,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.of(visitor_id, "1", client_type, "iphone")
        )
    );
    index.add(
        new MapBasedInputRow(
            timestamp + 2,
            Lists.newArrayList(visitor_id, client_type),
            ImmutableMap.of(visitor_id, "2", client_type, "android")
        )
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimensions(new DefaultDimensionSpec(
            client_type,
            client_type
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec(client_type, OrderByColumnSpec.Direction.DESCENDING)),
                10
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new DistinctCountAggregatorFactory("UV", visitor_id, null))
        .build();
    final Segment incrementalIndexSegment = new IncrementalIndexSegment(index, null);

    Iterable<ResultRow> results = FluentQueryRunner
        .create(factory.createRunner(incrementalIndexSegment), factory.getToolchest())
        .applyPreMergeDecoration()
        .mergeResults(true)
        .applyPostMergeDecoration()
        .run(QueryPlus.wrap(GroupByQueryRunnerTestHelper.populateResourceId(query)))
        .toList();

    List<ResultRow> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970-01-01T00:00:00.000Z",
            client_type, "iphone",
            "UV", 2L,
            "rows", 2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970-01-01T00:00:00.000Z",
            client_type, "android",
            "UV", 1L,
            "rows", 1L
        )
    );
    TestHelper.assertExpectedObjects(expectedResults, results, "distinct-count");
  }

  @Test
  public void testWithName()
  {
    DistinctCountAggregatorFactory aggregatorFactory = new DistinctCountAggregatorFactory(
        "distinct",
        "visitor_id",
        null
    );
    Assert.assertEquals(aggregatorFactory, aggregatorFactory.withName("distinct"));
    Assert.assertEquals("newTest", aggregatorFactory.withName("newTest").getName());
  }
}

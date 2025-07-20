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

package org.apache.robux.query.search;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.metrics.StubServiceEmitter;
import org.apache.robux.query.DefaultQueryMetricsTest;
import org.apache.robux.query.RobuxMetrics;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.QueryRunnerTestHelper;
import org.apache.robux.query.dimension.DefaultDimensionSpec;
import org.apache.robux.query.dimension.ListFilteredDimensionSpec;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultSearchQueryMetricsTest extends InitializedNullHandlingTest
{

  /**
   * Tests that passed a query {@link DefaultSearchQueryMetrics} produces events with a certain set of dimensions.
   */
  @Test
  public void testDefaultSearchQueryMetricsQuery()
  {
    final StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    SearchQuery query = Robuxs
        .newSearchQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .dimensions(new ListFilteredDimensionSpec(
            new DefaultDimensionSpec("tags", "tags"),
            ImmutableSet.of("t3"),
            null
        ))
        .context(ImmutableMap.of("testKey", "testValue"))
        .build();

    SearchQueryMetrics queryMetrics = DefaultSearchQueryMetricsFactory.instance().makeMetrics(query);

    queryMetrics.query(query);

    queryMetrics.reportQueryTime(0).emit(serviceEmitter);
    Map<String, Object> actualEvent = serviceEmitter.getEvents().get(0).toMap();
    Assert.assertEquals(13, actualEvent.size());
    Assert.assertTrue(actualEvent.containsKey("feed"));
    Assert.assertTrue(actualEvent.containsKey("timestamp"));
    Assert.assertEquals("", actualEvent.get("host"));
    Assert.assertEquals("", actualEvent.get("service"));
    Assert.assertEquals(QueryRunnerTestHelper.DATA_SOURCE, actualEvent.get(RobuxMetrics.DATASOURCE));
    Assert.assertEquals(query.getType(), actualEvent.get(RobuxMetrics.TYPE));
    List<Interval> expectedIntervals = QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC.getIntervals();
    List<String> expectedStringIntervals =
        expectedIntervals.stream().map(Interval::toString).collect(Collectors.toList());
    Assert.assertEquals(expectedStringIntervals, actualEvent.get(RobuxMetrics.INTERVAL));
    Assert.assertEquals("false", actualEvent.get("hasFilters"));
    Assert.assertEquals(expectedIntervals.get(0).toDuration().toString(), actualEvent.get("duration"));
    Assert.assertEquals("", actualEvent.get(RobuxMetrics.ID));
    Assert.assertEquals(ImmutableMap.of("testKey", "testValue"), actualEvent.get("context"));

    // Metric
    Assert.assertEquals("query/time", actualEvent.get("metric"));
    Assert.assertEquals(0L, actualEvent.get("value"));

    Assert.assertThrows(ISE.class, () -> queryMetrics.sqlQueryId("dummy"));
  }

  @Test
  public void testDefaultSearchQueryMetricsMetricNamesAndUnits()
  {
    SearchQuery query = Robuxs
        .newSearchQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .build();

    SearchQueryMetrics queryMetrics = DefaultSearchQueryMetricsFactory.instance().makeMetrics(query);
    DefaultQueryMetricsTest.testQueryMetricsDefaultMetricNamesAndUnits(queryMetrics);
  }
}

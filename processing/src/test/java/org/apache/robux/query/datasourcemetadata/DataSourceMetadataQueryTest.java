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

package org.apache.robux.query.datasourcemetadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.data.input.MapBasedInputRow;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.jackson.JacksonUtils;
import org.apache.robux.query.DefaultGenericQueryMetricsFactory;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.GenericQueryMetricsFactory;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryContext;
import org.apache.robux.query.QueryContexts;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerTestHelper;
import org.apache.robux.query.Result;
import org.apache.robux.query.aggregation.CountAggregatorFactory;
import org.apache.robux.query.context.ConcurrentResponseContext;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.segment.IncrementalIndexSegment;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.QueryableIndexSegment;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.incremental.IncrementalIndex;
import org.apache.robux.segment.incremental.OnheapIncrementalIndex;
import org.apache.robux.timeline.LogicalSegment;
import org.apache.robux.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DataSourceMetadataQueryTest
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query<?> query = Robuxs.newDataSourceMetadataQueryBuilder()
                           .dataSource("testing")
                           .build();

    String json = JSON_MAPPER.writeValueAsString(query);
    Query<?> serdeQuery = JSON_MAPPER.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  @Test
  public void testContextSerde() throws Exception
  {
    final DataSourceMetadataQuery query = Robuxs.newDataSourceMetadataQueryBuilder()
                                                .dataSource("foo")
                                                .intervals("2013/2014")
                                                .context(
                                                    ImmutableMap.of(
                                                        QueryContexts.PRIORITY_KEY,
                                                        1,
                                                        QueryContexts.USE_CACHE_KEY,
                                                        true,
                                                        QueryContexts.POPULATE_CACHE_KEY,
                                                        "true",
                                                        QueryContexts.FINALIZE_KEY,
                                                        true
                                                    )
                                                ).build();

    final ObjectMapper mapper = new DefaultObjectMapper();

    final Query<?> serdeQuery = mapper.readValue(
        mapper.writeValueAsBytes(
            mapper.readValue(
                mapper.writeValueAsString(
                    query
                ), Query.class
            )
        ), Query.class
    );

    final QueryContext queryContext = serdeQuery.context();
    Assert.assertEquals(1, (int) queryContext.getInt(QueryContexts.PRIORITY_KEY));
    Assert.assertEquals(true, queryContext.getBoolean(QueryContexts.USE_CACHE_KEY));
    Assert.assertEquals("true", queryContext.getString(QueryContexts.POPULATE_CACHE_KEY));
    Assert.assertEquals(true, queryContext.getBoolean(QueryContexts.FINALIZE_KEY));
    Assert.assertEquals(true, queryContext.getBoolean(QueryContexts.USE_CACHE_KEY, false));
    Assert.assertEquals(true, queryContext.getBoolean(QueryContexts.POPULATE_CACHE_KEY, false));
    Assert.assertEquals(true, queryContext.getBoolean(QueryContexts.FINALIZE_KEY, false));
  }

  /**
   * Build an index using a row with the provided event timestamp.
   */
  private IncrementalIndex buildIndex(final DateTime eventTimestamp)
  {
    final IncrementalIndex rtIndex = new OnheapIncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(new CountAggregatorFactory("count"))
        .setMaxRowCount(1000)
        .build();
    rtIndex.add(
        new MapBasedInputRow(
            eventTimestamp.getMillis(),
            ImmutableList.of("dim1"),
            ImmutableMap.of("dim1", "x")
        )
    );
    return rtIndex;
  }

  @Test
  public void testMaxIngestedEventTimeIncrementalIndex()
  {
    final DateTime timestamp = DateTimes.of("2020-01-02T03:04:05.678Z");
    final IncrementalIndex rtIndex = buildIndex(timestamp);
    DataSourceMetadataQuery dataSourceMetadataQuery =
        Robuxs.newDataSourceMetadataQueryBuilder()
              .dataSource("testing")
              .build();
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.initializeMissingSegments();

    final QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        new DataSourceMetadataQueryRunnerFactory(
            new DataSourceQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        new IncrementalIndexSegment(rtIndex, SegmentId.dummy("test")),
        null
    );

    Iterable<Result<DataSourceMetadataResultValue>> results =
        runner.run(QueryPlus.wrap(dataSourceMetadataQuery), context).toList();
    DataSourceMetadataResultValue val = results.iterator().next().getValue();
    DateTime maxIngestedEventTime = val.getMaxIngestedEventTime();

    Assert.assertEquals(timestamp, maxIngestedEventTime);
  }

  @Test
  public void testMaxIngestedEventTimeQueryableIndex()
  {
    final DateTime timestamp = DateTimes.of("2020-01-02T03:04:05.678Z");
    final QueryableIndex queryableIndex = TestIndex.persistAndMemoryMap(buildIndex(timestamp));
    DataSourceMetadataQuery dataSourceMetadataQuery =
        Robuxs.newDataSourceMetadataQueryBuilder()
              .dataSource("testing")
              .build();
    ResponseContext context = ConcurrentResponseContext.createEmpty();
    context.initializeMissingSegments();

    final QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        new DataSourceMetadataQueryRunnerFactory(
            new DataSourceQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        new QueryableIndexSegment(queryableIndex, SegmentId.dummy("test")),
        null
    );

    Iterable<Result<DataSourceMetadataResultValue>> results =
        runner.run(QueryPlus.wrap(dataSourceMetadataQuery), context).toList();
    DataSourceMetadataResultValue val = results.iterator().next().getValue();
    DateTime maxIngestedEventTime = val.getMaxIngestedEventTime();

    Assert.assertEquals(timestamp, maxIngestedEventTime);
  }

  @Test
  public void testFilterSegments()
  {
    GenericQueryMetricsFactory queryMetricsFactory = DefaultGenericQueryMetricsFactory.instance();
    DataSourceQueryQueryToolChest toolChest = new DataSourceQueryQueryToolChest(queryMetricsFactory);
    List<LogicalSegment> segments = toolChest
        .filterSegments(
            null,
            Arrays.asList(
                new LogicalSegment()
                {
                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2012-01-01/P1D");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return getInterval();
                  }
                },
                new LogicalSegment()
                {
                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2012-01-01T01/PT1H");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return getInterval();
                  }
                },
                new LogicalSegment()
                {
                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2013-01-01/P1D");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return getInterval();
                  }
                },
                new LogicalSegment()
                {
                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2013-01-01T01/PT1H");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return getInterval();
                  }
                },
                new LogicalSegment()
                {
                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2013-01-01T02/PT1H");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return getInterval();
                  }
                }
            )
        );

    Assert.assertEquals(segments.size(), 2);
    // should only have the latest segments.
    List<LogicalSegment> expected = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2013-01-01/P1D");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2013-01-01T02/PT1H");
          }

          @Override
          public Interval getTrueInterval()
          {
            return getInterval();
          }
        }
    );

    for (int i = 0; i < segments.size(); i++) {
      Assert.assertEquals(expected.get(i).getInterval(), segments.get(i).getInterval());
    }
  }

  @Test
  public void testFilterOverlappingSegments()
  {
    final GenericQueryMetricsFactory queryMetricsFactory = DefaultGenericQueryMetricsFactory.instance();
    final DataSourceQueryQueryToolChest toolChest = new DataSourceQueryQueryToolChest(queryMetricsFactory);
    final List<LogicalSegment> segments = toolChest
        .filterSegments(
            null,
            ImmutableList.of(
                new LogicalSegment()
                {
                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2015/2016-08-01");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return Intervals.of("2015/2016-08-01");
                  }
                },
                new LogicalSegment()
                {
                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2016-08-01/2017");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return Intervals.of("2016-08-01/2017");
                  }
                },
                new LogicalSegment()
                {
                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2017/2017-08-01");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return Intervals.of("2017/2018");
                  }
                },
                new LogicalSegment()
                {

                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2017-08-01/2017-08-02");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return Intervals.of("2017-08-01/2017-08-02");
                  }
                },
                new LogicalSegment()
                {
                  @Override
                  public Interval getInterval()
                  {
                    return Intervals.of("2017-08-02/2018");
                  }

                  @Override
                  public Interval getTrueInterval()
                  {
                    return Intervals.of("2017/2018");
                  }
                }
            )
        );

    final List<LogicalSegment> expected = ImmutableList.of(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2017/2017-08-01");
          }

          @Override
          public Interval getTrueInterval()
          {
            return Intervals.of("2017/2018");
          }
        },
        new LogicalSegment()
        {

          @Override
          public Interval getInterval()
          {
            return Intervals.of("2017-08-01/2017-08-02");
          }

          @Override
          public Interval getTrueInterval()
          {
            return Intervals.of("2017-08-01/2017-08-02");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return Intervals.of("2017-08-02/2018");
          }

          @Override
          public Interval getTrueInterval()
          {
            return Intervals.of("2017/2018");
          }
        }
    );

    Assert.assertEquals(expected.size(), segments.size());

    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(expected.get(i).getInterval(), segments.get(i).getInterval());
      Assert.assertEquals(expected.get(i).getTrueInterval(), segments.get(i).getTrueInterval());
    }
  }

  @Test
  public void testResultSerialization()
  {
    final DataSourceMetadataResultValue resultValue = new DataSourceMetadataResultValue(DateTimes.of("2000-01-01T00Z"));
    final Map<String, Object> resultValueMap = new DefaultObjectMapper().convertValue(
        resultValue,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assert.assertEquals(
        ImmutableMap.<String, Object>of("maxIngestedEventTime", "2000-01-01T00:00:00.000Z"),
        resultValueMap
    );
  }

  @Test
  public void testResultDeserialization()
  {
    final Map<String, Object> resultValueMap = ImmutableMap.of(
        "maxIngestedEventTime",
        "2000-01-01T00:00:00.000Z"
    );
    final DataSourceMetadataResultValue resultValue = new DefaultObjectMapper().convertValue(
        resultValueMap,
        DataSourceMetadataResultValue.class
    );
    Assert.assertEquals(DateTimes.of("2000"), resultValue.getMaxIngestedEventTime());
  }

}

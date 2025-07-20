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

package org.apache.robux.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.data.input.impl.DimensionsSpec;
import org.apache.robux.data.input.impl.InputRowParser;
import org.apache.robux.data.input.impl.JSONParseSpec;
import org.apache.robux.data.input.impl.MapInputRowParser;
import org.apache.robux.data.input.impl.TimestampSpec;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.FileUtils;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.robux.query.metadata.SegmentMetadataQueryConfig;
import org.apache.robux.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.robux.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.robux.query.metadata.metadata.ColumnAnalysis;
import org.apache.robux.query.metadata.metadata.ListColumnIncluderator;
import org.apache.robux.query.metadata.metadata.SegmentAnalysis;
import org.apache.robux.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.robux.query.scan.ScanQuery;
import org.apache.robux.query.scan.ScanQueryConfig;
import org.apache.robux.query.scan.ScanQueryEngine;
import org.apache.robux.query.scan.ScanQueryQueryToolChest;
import org.apache.robux.query.scan.ScanQueryRunnerFactory;
import org.apache.robux.query.scan.ScanQueryRunnerTest;
import org.apache.robux.query.scan.ScanResultValue;
import org.apache.robux.query.spec.LegacySegmentSpec;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMergerV9;
import org.apache.robux.segment.IndexSpec;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.QueryableIndexSegment;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.column.ColumnHolder;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.column.ValueType;
import org.apache.robux.segment.incremental.IncrementalIndex;
import org.apache.robux.segment.incremental.IncrementalIndexSchema;
import org.apache.robux.segment.incremental.OnheapIncrementalIndex;
import org.apache.robux.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.apache.robux.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class DoubleStorageTest extends InitializedNullHandlingTest
{

  private static final SegmentMetadataQueryRunnerFactory METADATA_QR_FACTORY = new SegmentMetadataQueryRunnerFactory(
      new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );

  private static final ScanQueryQueryToolChest SCAN_QUERY_QUERY_TOOL_CHEST = new ScanQueryQueryToolChest(
      DefaultGenericQueryMetricsFactory.instance()
  );

  private static final ScanQueryRunnerFactory SCAN_QUERY_RUNNER_FACTORY = new ScanQueryRunnerFactory(
      SCAN_QUERY_QUERY_TOOL_CHEST,
      new ScanQueryEngine(),
      new ScanQueryConfig()
  );

  private Robuxs.ScanQueryBuilder newTestQuery()
  {
    return Robuxs.newScanQueryBuilder()
                 .dataSource(new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE))
                 .columns(Collections.emptyList())
                 .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                 .limit(Integer.MAX_VALUE);
  }


  private static final IndexMergerV9 INDEX_MERGER_V9 =
      TestHelper.getTestIndexMergerV9(OffHeapMemorySegmentWriteOutMediumFactory.instance());
  private static final IndexIO INDEX_IO = TestHelper.getTestIndexIO();
  private static final Integer MAX_ROWS = 10;
  private static final String TIME_COLUMN = "__time";
  private static final String DIM_NAME = "testDimName";
  private static final String DIM_VALUE = "testDimValue";
  private static final String DIM_FLOAT_NAME = "testDimFloatName";
  private static final SegmentId SEGMENT_ID = SegmentId.dummy("segmentId");
  private static final Interval INTERVAL = Intervals.of("2011-01-13T00:00:00.000Z/2011-01-22T00:00:00.001Z");

  private static final InputRowParser<Map<String, Object>> ROW_PARSER = new MapInputRowParser(
      new JSONParseSpec(
          new TimestampSpec(TIME_COLUMN, "auto", null),
          DimensionsSpec.builder()
                        .setDimensions(DimensionsSpec.getDefaultSchemas(ImmutableList.of(DIM_NAME)))
                        .setDimensionExclusions(ImmutableList.of(DIM_FLOAT_NAME))
                        .build(),
          null,
          null,
          null
      )
  );

  private QueryableIndex index;
  private final SegmentAnalysis expectedSegmentAnalysis;
  private final String storeDoubleAs;

  public DoubleStorageTest(
      String storeDoubleAs,
      SegmentAnalysis expectedSegmentAnalysis
  )
  {
    this.storeDoubleAs = storeDoubleAs;
    this.expectedSegmentAnalysis = expectedSegmentAnalysis;
  }

  @Parameterized.Parameters
  public static Collection<?> dataFeeder()
  {
    SegmentAnalysis expectedSegmentAnalysisDouble = new SegmentAnalysis(
        SEGMENT_ID.toString(),
        ImmutableList.of(INTERVAL),
        new LinkedHashMap<>(
            ImmutableMap.of(
                TIME_COLUMN,
                new ColumnAnalysis(
                    ColumnType.LONG,
                    ValueType.LONG.name(),
                    false,
                    false,
                    100,
                    null,
                    null,
                    null,
                    null
                ),
                DIM_NAME,
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.name(),
                    false,
                    false,
                    120,
                    1,
                    DIM_VALUE,
                    DIM_VALUE,
                    null
                ),
                DIM_FLOAT_NAME,
                new ColumnAnalysis(
                    ColumnType.DOUBLE,
                    ValueType.DOUBLE.name(),
                    false,
                    false,
                    80,
                    null,
                    null,
                    null,
                    null
                )
            )
        ), 330,
        MAX_ROWS,
        null,
        null,
        null,
        null,
        null
    );

    SegmentAnalysis expectedSegmentAnalysisFloat = new SegmentAnalysis(
        SEGMENT_ID.toString(),
        ImmutableList.of(INTERVAL),
        new LinkedHashMap<>(
            ImmutableMap.of(
                TIME_COLUMN,
                new ColumnAnalysis(
                    ColumnType.LONG,
                    ValueType.LONG.name(),
                    false,
                    false,
                    100,
                    null,
                    null,
                    null,
                    null
                ),
                DIM_NAME,
                new ColumnAnalysis(
                    ColumnType.STRING,
                    ValueType.STRING.name(),
                    false,
                    false,
                    120,
                    1,
                    DIM_VALUE,
                    DIM_VALUE,
                    null
                ),
                DIM_FLOAT_NAME,
                new ColumnAnalysis(
                    ColumnType.FLOAT,
                    ValueType.FLOAT.name(),
                    false,
                    false,
                    80,
                    null,
                    null,
                    null,
                    null
                )
            )
        ),
        330,
        MAX_ROWS,
        null,
        null,
        null,
        null,
        null
    );

    return ImmutableList.of(
        new Object[]{"double", expectedSegmentAnalysisDouble},
        new Object[]{"float", expectedSegmentAnalysisFloat}
    );
  }

  @Before
  public void setup() throws IOException
  {
    index = buildIndex(storeDoubleAs);
  }

  @Test
  public void testMetaDataAnalysis()
  {
    QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        METADATA_QR_FACTORY,
        SEGMENT_ID,
        new QueryableIndexSegment(index, SEGMENT_ID),
        null
    );


    SegmentMetadataQuery segmentMetadataQuery = Robuxs.newSegmentMetadataQueryBuilder()
                                                      .dataSource("testing")
                                                      .intervals(ImmutableList.of(INTERVAL))
                                                      .toInclude(new ListColumnIncluderator(Arrays.asList(
                                                          TIME_COLUMN,
                                                          DIM_NAME,
                                                          DIM_FLOAT_NAME
                                                      )))
                                                      .analysisTypes(
                                                          SegmentMetadataQuery.AnalysisType.CARDINALITY,
                                                          SegmentMetadataQuery.AnalysisType.SIZE,
                                                          SegmentMetadataQuery.AnalysisType.INTERVAL,
                                                          SegmentMetadataQuery.AnalysisType.MINMAX
                                                      )
                                                      .merge(true)
                                                      .build();
    List<SegmentAnalysis> results = runner.run(QueryPlus.wrap(segmentMetadataQuery)).toList();

    Assert.assertEquals(Collections.singletonList(expectedSegmentAnalysis), results);

  }

  @Test
  public void testSelectValues()
  {
    QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        SCAN_QUERY_RUNNER_FACTORY,
        SEGMENT_ID,
        new QueryableIndexSegment(index, SEGMENT_ID),
        null
    );

    ScanQuery query = newTestQuery()
        .intervals(new LegacySegmentSpec(INTERVAL))
        .virtualColumns()
        .build();

    Iterable<ScanResultValue> results = runner.run(QueryPlus.wrap(query)).toList();

    ScanResultValue expectedScanResult = new ScanResultValue(
        SEGMENT_ID.toString(),
        ImmutableList.of(TIME_COLUMN, DIM_NAME, DIM_FLOAT_NAME),
        getStreamOfEvents().collect(Collectors.toList())
    );
    List<ScanResultValue> expectedResults = Collections.singletonList(expectedScanResult);
    ScanQueryRunnerTest.verify(expectedResults, results);
  }

  private static QueryableIndex buildIndex(String storeDoubleAsFloat) throws IOException
  {
    String oldValue = System.getProperty(ColumnHolder.DOUBLE_STORAGE_TYPE_PROPERTY);
    System.setProperty(ColumnHolder.DOUBLE_STORAGE_TYPE_PROPERTY, storeDoubleAsFloat);
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of("2011-01-13T00:00:00.000Z").getMillis())
        .withDimensionsSpec(ROW_PARSER)
        .withMetrics(
            new DoubleSumAggregatorFactory(DIM_FLOAT_NAME, DIM_FLOAT_NAME)
        )
        .build();

    final IncrementalIndex index = new OnheapIncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(MAX_ROWS)
        .build();


    getStreamOfEvents().forEach(o -> {
      index.add(ROW_PARSER.parseBatch((Map<String, Object>) o).get(0));
    });

    if (oldValue == null) {
      System.clearProperty(ColumnHolder.DOUBLE_STORAGE_TYPE_PROPERTY);
    } else {
      System.setProperty(ColumnHolder.DOUBLE_STORAGE_TYPE_PROPERTY, oldValue);
    }
    File someTmpFile = File.createTempFile("billy", "yay");
    someTmpFile.delete();
    FileUtils.mkdirp(someTmpFile);
    INDEX_MERGER_V9.persist(index, someTmpFile, IndexSpec.DEFAULT, null);
    someTmpFile.delete();
    return INDEX_IO.loadIndex(someTmpFile);
  }

  @After
  public void cleanUp()
  {
    index.close();
  }

  private static Stream getStreamOfEvents()
  {
    return IntStream.range(0, MAX_ROWS).mapToObj(i -> ImmutableMap.of(
        TIME_COLUMN, DateTimes.of("2011-01-13T00:00:00.000Z").plusDays(i).getMillis(),
        DIM_NAME, DIM_VALUE,
        DIM_FLOAT_NAME, i / 1.6179775280898876
    ));
  }

}

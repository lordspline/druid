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

package org.apache.robux.indexer.hadoop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.robux.collections.spatial.search.RadiusBound;
import org.apache.robux.data.input.InputRow;
import org.apache.robux.data.input.impl.DelimitedParseSpec;
import org.apache.robux.data.input.impl.DimensionsSpec;
import org.apache.robux.data.input.impl.NewSpatialDimensionSchema;
import org.apache.robux.data.input.impl.StringDimensionSchema;
import org.apache.robux.data.input.impl.StringInputRowParser;
import org.apache.robux.data.input.impl.TimestampSpec;
import org.apache.robux.hll.HyperLogLogCollector;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.query.aggregation.AggregatorFactory;
import org.apache.robux.query.aggregation.LongSumAggregatorFactory;
import org.apache.robux.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.robux.query.filter.SpatialDimFilter;
import org.apache.robux.segment.CursorFactory;
import org.apache.robux.segment.IncrementalIndexSegment;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMerger;
import org.apache.robux.segment.IndexSpec;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.QueryableIndexCursorFactory;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.incremental.IncrementalIndex;
import org.apache.robux.segment.incremental.IncrementalIndexSchema;
import org.apache.robux.segment.incremental.OnheapIncrementalIndex;
import org.apache.robux.segment.realtime.WindowedCursorFactory;
import org.apache.robux.segment.transform.TransformSpec;
import org.apache.robux.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.robux.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.robux.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.robux.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 */
@RunWith(Parameterized.class)
public class DatasourceRecordReaderSegmentReaderTest
{
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      ImmutableList.of(
          new StringDimensionSchema("host"),
          new NewSpatialDimensionSchema("spatial", ImmutableList.of("x", "y"))
      )
  );

  private static final DimensionsSpec DIMENSIONS_SPEC_REINDEX = new DimensionsSpec(
      ImmutableList.of(
          new StringDimensionSchema("host"),
          new NewSpatialDimensionSchema("spatial", ImmutableList.of("spatial"))
      )
  );

  private static final List<AggregatorFactory> AGGREGATORS = ImmutableList.of(
      new LongSumAggregatorFactory("visited_sum", "visited"),
      new HyperUniquesAggregatorFactory("unique_hosts", "host")
  );

  private static final List<AggregatorFactory> AGGREGATORS_REINDEX = ImmutableList.of(
      new LongSumAggregatorFactory("visited_sum", "visited_sum"),
      new HyperUniquesAggregatorFactory("unique_hosts", "unique_hosts")
  );

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[] {TmpFileSegmentWriteOutMediumFactory.instance()},
        new Object[] {OffHeapMemorySegmentWriteOutMediumFactory.instance()}
    );
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private final IndexIO indexIO;
  private final IndexMerger indexMerger;

  public DatasourceRecordReaderSegmentReaderTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    indexIO = TestHelper.getTestIndexIO();
    indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
  }

  @Test
  public void testReadFromIndexAndWriteAnotherIndex() throws Exception
  {
    // Tests a "reindexing" use case that is a common use of ingestSegment.

    File segmentDir = tempFolder.newFolder();
    createTestIndex(segmentDir);

    try (
        final QueryableIndex qi = indexIO.loadIndex(segmentDir);
        final IncrementalIndex index = new OnheapIncrementalIndex.Builder()
            .setIndexSchema(
                new IncrementalIndexSchema.Builder()
                    .withDimensionsSpec(DIMENSIONS_SPEC_REINDEX)
                    .withMetrics(AGGREGATORS_REINDEX.toArray(new AggregatorFactory[0]))
                    .build()
            )
            .setMaxRowCount(5000)
            .build()
    ) {
      final WindowedCursorFactory ws = new WindowedCursorFactory(
          new QueryableIndexCursorFactory(qi),
          qi.getDataInterval()
      );
      final DatasourceRecordReader.SegmentReader segmentReader = new DatasourceRecordReader.SegmentReader(
          ImmutableList.of(ws, ws),
          TransformSpec.NONE,
          ImmutableList.of("host", "spatial"),
          ImmutableList.of("visited_sum", "unique_hosts"),
          null
      );

      int count = 0;
      while (segmentReader.hasMore()) {
        final InputRow row = segmentReader.nextRow();
        Assert.assertNotNull(row);
        if (count == 0) {
          Assert.assertEquals(DateTimes.of("2014-10-22T00Z"), row.getTimestamp());
          Assert.assertEquals("host1", row.getRaw("host"));
          Assert.assertEquals("0,1", row.getRaw("spatial"));
          Assert.assertEquals(10L, row.getRaw("visited_sum"));
          Assert.assertEquals(1.0d, ((HyperLogLogCollector) row.getRaw("unique_hosts")).estimateCardinality(), 0.1);
        }
        count++;
        index.add(row);
      }
      Assert.assertEquals(18, count);

      // Check the index
      Assert.assertEquals(9, index.numRows());
      final IncrementalIndexSegment queryable = new IncrementalIndexSegment(index, SegmentId.dummy("test"));
      final List<String> dimensions = index.getDimensionNames(false);
      Assert.assertEquals(2, dimensions.size());
      Assert.assertEquals("host", dimensions.get(0));
      Assert.assertEquals("spatial", dimensions.get(1));
      Assert.assertEquals(
          ImmutableList.of("visited_sum", "unique_hosts"),
          Arrays.stream(index.getMetricAggs()).map(AggregatorFactory::getName).collect(Collectors.toList())
      );

      // Do a spatial filter
      final DatasourceRecordReader.SegmentReader segmentReader2 = new DatasourceRecordReader.SegmentReader(
          ImmutableList.of(new WindowedCursorFactory(queryable.as(CursorFactory.class), Intervals.of("2000/3000"))),
          TransformSpec.NONE,
          ImmutableList.of("host", "spatial"),
          ImmutableList.of("visited_sum", "unique_hosts"),
          new SpatialDimFilter("spatial", new RadiusBound(new float[]{1, 0}, 0.1f))
      );
      final InputRow row = segmentReader2.nextRow();
      Assert.assertFalse(segmentReader2.hasMore());
      Assert.assertEquals(DateTimes.of("2014-10-22T00Z"), row.getTimestamp());
      Assert.assertEquals("host2", row.getRaw("host"));
      Assert.assertEquals("1,0", row.getRaw("spatial"));
      Assert.assertEquals(40L, row.getRaw("visited_sum"));
      Assert.assertEquals(1.0d, ((HyperLogLogCollector) row.getRaw("unique_hosts")).estimateCardinality(), 0.1);
    }
  }

  private void createTestIndex(File segmentDir) throws Exception
  {
    final List<String> rows = Lists.newArrayList(
        "2014102200\thost1\t10\t0\t1",
        "2014102200\thost2\t20\t1\t0",
        "2014102200\thost3\t30\t1\t1",
        "2014102201\thost1\t10\t1\t1",
        "2014102201\thost2\t20\t1\t1",
        "2014102201\thost3\t30\t1\t1",
        "2014102202\thost1\t10\t1\t1",
        "2014102202\thost2\t20\t1\t1",
        "2014102202\thost3\t30\t1\t1"
    );

    final StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            DIMENSIONS_SPEC,
            "\t",
            null,
            ImmutableList.of("timestamp", "host", "visited", "x", "y", "spatial"),
            false,
            0
        ),
        StandardCharsets.UTF_8.toString()
    );

    try (
        final IncrementalIndex index = new OnheapIncrementalIndex.Builder()
            .setIndexSchema(
                new IncrementalIndexSchema.Builder()
                    .withDimensionsSpec(parser.getParseSpec().getDimensionsSpec())
                    .withMetrics(AGGREGATORS.toArray(new AggregatorFactory[0]))
                    .build()
            )
            .setMaxRowCount(5000)
            .build()
    ) {
      for (String line : rows) {
        index.add(parser.parse(line));
      }
      indexMerger.persist(index, segmentDir, IndexSpec.DEFAULT, null);
    }
  }
}

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

package org.apache.robux.msq.querykit.scan;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.robux.collections.ReferenceCountingResourceHolder;
import org.apache.robux.collections.ResourceHolder;
import org.apache.robux.collections.StupidResourceHolder;
import org.apache.robux.frame.Frame;
import org.apache.robux.frame.FrameType;
import org.apache.robux.frame.allocation.ArenaMemoryAllocator;
import org.apache.robux.frame.allocation.HeapMemoryAllocator;
import org.apache.robux.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.robux.frame.channel.BlockingQueueFrameChannel;
import org.apache.robux.frame.channel.WritableFrameChannel;
import org.apache.robux.frame.read.FrameReader;
import org.apache.robux.frame.testutil.FrameSequenceBuilder;
import org.apache.robux.frame.testutil.FrameTestUtil;
import org.apache.robux.frame.write.FrameWriterFactory;
import org.apache.robux.frame.write.FrameWriters;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.Unit;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.msq.input.ReadableInput;
import org.apache.robux.msq.input.table.RichSegmentDescriptor;
import org.apache.robux.msq.input.table.SegmentWithDescriptor;
import org.apache.robux.msq.kernel.StageId;
import org.apache.robux.msq.kernel.StagePartition;
import org.apache.robux.msq.querykit.FrameProcessorTestBase;
import org.apache.robux.msq.test.LimitedFrameWriterFactory;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.scan.ScanQuery;
import org.apache.robux.query.spec.MultipleIntervalSegmentSpec;
import org.apache.robux.segment.CompleteSegment;
import org.apache.robux.segment.CursorFactory;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.QueryableIndexCursorFactory;
import org.apache.robux.segment.QueryableIndexSegment;
import org.apache.robux.segment.SegmentMapFunction;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.robux.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ScanQueryFrameProcessorTest extends FrameProcessorTestBase
{

  @Test
  public void test_runWithSegments() throws Exception
  {
    final QueryableIndex queryableIndex = TestIndex.getMMappedTestIndex();

    final CursorFactory cursorFactory =
        new QueryableIndexCursorFactory(queryableIndex);

    // put funny intervals on query to ensure it is adjusted to the segment interval before building cursor
    final ScanQuery query =
        Robuxs.newScanQueryBuilder()
              .dataSource("test")
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      ImmutableList.of(
                          Intervals.of("2001-01-01T00Z/2011-01-01T00Z"),
                          Intervals.of("2011-01-02T00Z/2021-01-01T00Z")
                      )
                  )
              )
              .columns(cursorFactory.getRowSignature().getColumnNames())
              .build();

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    // Limit output frames to 1 row to ensure we test edge cases
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeFrameWriterFactory(
            FrameType.latestRowBased(),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            cursorFactory.getRowSignature(),
            Collections.emptyList(),
            false
        ),
        1
    );

    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        null,
        new DefaultObjectMapper(),
        ReadableInput.segment(
            new SegmentWithDescriptor(
                () -> new StupidResourceHolder<>(new CompleteSegment(null, new QueryableIndexSegment(queryableIndex, SegmentId.dummy("test")))),
                new RichSegmentDescriptor(queryableIndex.getDataInterval(), queryableIndex.getDataInterval(), "dummy_version", 0)
            )
        ),
        SegmentMapFunction.IDENTITY,
        new ResourceHolder<>()
        {
          @Override
          public WritableFrameChannel get()
          {
            return outputChannel.writable();
          }

          @Override
          public void close()
          {
            try {
              outputChannel.writable().close();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        },
        new ReferenceCountingResourceHolder<>(frameWriterFactory, () -> {})
    );

    ListenableFuture<Object> retVal = exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(cursorFactory.getRowSignature())
    );

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromCursorFactory(cursorFactory, cursorFactory.getRowSignature(), false),
        rowsFromProcessor
    );

    Assert.assertEquals(Unit.instance(), retVal.get());
  }

  @Test
  public void test_runWithInputChannel() throws Exception
  {
    final CursorFactory cursorFactory =
        new IncrementalIndexCursorFactory(TestIndex.getIncrementalTestIndex());

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromCursorFactory(cursorFactory)
                            .maxRowsPerFrame(5)
                            .frameType(FrameType.latestRowBased())
                            .allocator(ArenaMemoryAllocator.createOnHeap(100_000));

    final RowSignature signature = frameSequenceBuilder.signature();
    final List<Frame> frames = frameSequenceBuilder.frames().toList();
    final BlockingQueueFrameChannel inputChannel = new BlockingQueueFrameChannel(frames.size());
    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    try (final WritableFrameChannel writableInputChannel = inputChannel.writable()) {
      for (final Frame frame : frames) {
        writableInputChannel.write(frame);
      }
    }

    // put funny intervals on query to ensure it is validated before building cursor
    final ScanQuery query =
        Robuxs.newScanQueryBuilder()
              .dataSource("test")
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      ImmutableList.of(
                          Intervals.of("2001-01-01T00Z/2011-01-01T00Z"),
                          Intervals.of("2011-01-02T00Z/2021-01-01T00Z")
                      )
                  )
              )
              .columns(cursorFactory.getRowSignature().getColumnNames())
              .build();

    final StagePartition stagePartition = new StagePartition(new StageId("query", 0), 0);

    // Limit output frames to 1 row to ensure we test edge cases
    final FrameWriterFactory frameWriterFactory = new LimitedFrameWriterFactory(
        FrameWriters.makeFrameWriterFactory(
            FrameType.latestRowBased(),
            new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
            signature,
            Collections.emptyList(),
            false
        ),
        1
    );

    final ScanQueryFrameProcessor processor = new ScanQueryFrameProcessor(
        query,
        null,
        new DefaultObjectMapper(),
        ReadableInput.channel(inputChannel.readable(), FrameReader.create(signature), stagePartition),
        SegmentMapFunction.IDENTITY,
        new ResourceHolder<>()
        {
          @Override
          public WritableFrameChannel get()
          {
            return outputChannel.writable();
          }

          @Override
          public void close()
          {
            try {
              outputChannel.writable().close();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        },
        new ReferenceCountingResourceHolder<>(frameWriterFactory, () -> {})
    );

    ListenableFuture<Object> retVal = exec.runFully(processor, null);

    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        outputChannel.readable(),
        FrameReader.create(signature)
    );

    final RuntimeException e = Assert.assertThrows(
        RuntimeException.class,
        rowsFromProcessor::toList
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
            "Expected eternity intervals, but got[[2001-01-01T00:00:00.000Z/2011-01-01T00:00:00.000Z, "
            + "2011-01-02T00:00:00.000Z/2021-01-01T00:00:00.000Z]]"))
    );
  }
}

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

package org.apache.robux.msq.dart.worker;

import com.google.inject.Inject;
import org.apache.robux.collections.ReferenceCountingResourceHolder;
import org.apache.robux.collections.ResourceHolder;
import org.apache.robux.error.RobuxException;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.msq.counters.ChannelCounters;
import org.apache.robux.msq.querykit.DataSegmentProvider;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.segment.CompleteSegment;
import org.apache.robux.segment.PhysicalSegmentInspector;
import org.apache.robux.segment.ReferenceCountedSegmentProvider;
import org.apache.robux.segment.Segment;
import org.apache.robux.server.SegmentManager;
import org.apache.robux.timeline.SegmentId;
import org.apache.robux.timeline.VersionedIntervalTimeline;
import org.apache.robux.timeline.partition.PartitionChunk;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * Implementation of {@link DataSegmentProvider} that uses locally-cached segments from a {@link SegmentManager}.
 */
public class DartDataSegmentProvider implements DataSegmentProvider
{
  private final SegmentManager segmentManager;

  @Inject
  public DartDataSegmentProvider(SegmentManager segmentManager)
  {
    this.segmentManager = segmentManager;
  }

  @Override
  public Supplier<ResourceHolder<CompleteSegment>> fetchSegment(
      SegmentId segmentId,
      ChannelCounters channelCounters,
      boolean isReindex
  )
  {
    if (isReindex) {
      throw RobuxException.defensive("Got isReindex[%s], expected false", isReindex);
    }

    return () -> {
      final Optional<VersionedIntervalTimeline<String, ReferenceCountedSegmentProvider>> timeline =
          segmentManager.getTimeline(new TableDataSource(segmentId.getDataSource()));

      if (!timeline.isPresent()) {
        throw segmentNotFound(segmentId);
      }

      final PartitionChunk<ReferenceCountedSegmentProvider> chunk =
          timeline.get().findChunk(
              segmentId.getInterval(),
              segmentId.getVersion(),
              segmentId.getPartitionNum()
          );

      if (chunk == null) {
        throw segmentNotFound(segmentId);
      }

      final ReferenceCountedSegmentProvider segmentReference = chunk.getObject();
      final Optional<Segment> maybeSegment = segmentReference.acquireReference();
      if (!maybeSegment.isPresent()) {
        // Segment has disappeared before we could acquire a reference to it.
        throw segmentNotFound(segmentId);
      }
      final Segment segment = maybeSegment.get();

      final Closer closer = Closer.create();
      closer.register(() -> {
        final PhysicalSegmentInspector inspector = segment.as(PhysicalSegmentInspector.class);
        channelCounters.addFile(inspector != null ? inspector.getNumRows() : 0, 0);
        // don't release the reference until after we get the rows
        segment.close();
      });
      // we don't need to close CompleteSegment because the checked out reference is registered with the closer
      return new ReferenceCountingResourceHolder<>(new CompleteSegment(null, segment), closer);
    };
  }

  /**
   * Error to throw when a segment that was requested is not found. This can happen due to segment moves, etc.
   */
  private static RobuxException segmentNotFound(final SegmentId segmentId)
  {
    return RobuxException.forPersona(RobuxException.Persona.USER)
                         .ofCategory(RobuxException.Category.RUNTIME_FAILURE)
                         .build("Segment[%s] not found on this server. Please retry your query.", segmentId);
  }
}

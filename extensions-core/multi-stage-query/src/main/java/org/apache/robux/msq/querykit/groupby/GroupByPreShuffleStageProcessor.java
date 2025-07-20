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

package org.apache.robux.msq.querykit.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.robux.collections.ResourceHolder;
import org.apache.robux.frame.channel.WritableFrameChannel;
import org.apache.robux.frame.processor.FrameProcessor;
import org.apache.robux.frame.write.FrameWriterFactory;
import org.apache.robux.msq.exec.FrameContext;
import org.apache.robux.msq.input.ReadableInput;
import org.apache.robux.msq.querykit.BaseLeafStageProcessor;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.segment.SegmentMapFunction;

@JsonTypeName("groupByPreShuffle")
public class GroupByPreShuffleStageProcessor extends BaseLeafStageProcessor
{
  private final GroupByQuery query;

  @JsonCreator
  public GroupByPreShuffleStageProcessor(@JsonProperty("query") GroupByQuery query)
  {
    super(query);
    this.query = Preconditions.checkNotNull(query, "query");
  }

  @JsonProperty
  public GroupByQuery getQuery()
  {
    return query;
  }

  @Override
  protected FrameProcessor<Object> makeProcessor(
      final ReadableInput baseInput,
      final SegmentMapFunction segmentMapFn,
      final ResourceHolder<WritableFrameChannel> outputChannelHolder,
      final ResourceHolder<FrameWriterFactory> frameWriterFactoryHolder,
      final FrameContext frameContext
  )
  {
    return new GroupByPreShuffleFrameProcessor(
        query,
        frameContext.groupingEngine(),
        frameContext.processingBuffers().getBufferPool(),
        baseInput,
        segmentMapFn,
        outputChannelHolder,
        frameWriterFactoryHolder
    );
  }

  @Override
  public boolean usesProcessingBuffers()
  {
    return true;
  }
}

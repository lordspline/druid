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

package org.apache.robux.indexing.input;

import com.google.common.base.Preconditions;
import org.apache.robux.data.input.BytesCountingInputEntity;
import org.apache.robux.data.input.InputEntity;
import org.apache.robux.data.input.InputEntityReader;
import org.apache.robux.data.input.InputFormat;
import org.apache.robux.data.input.InputRowSchema;
import org.apache.robux.query.filter.DimFilter;
import org.apache.robux.segment.IndexIO;

import java.io.File;

public class RobuxSegmentInputFormat implements InputFormat
{
  private final IndexIO indexIO;
  private final DimFilter dimFilter;

  public RobuxSegmentInputFormat(
      IndexIO indexIO,
      DimFilter dimFilter
  )
  {
    this.indexIO = indexIO;
    this.dimFilter = dimFilter;
  }

  @Override
  public boolean isSplittable()
  {
    return false;
  }

  @Override
  public InputEntityReader createReader(
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory
  )
  {
    final InputEntity baseInputEntity;
    if (source instanceof BytesCountingInputEntity) {
      baseInputEntity = ((BytesCountingInputEntity) source).getBaseInputEntity();
    } else {
      baseInputEntity = source;
    }

    // this method handles the case when the entity comes from a tombstone or from a regular segment
    Preconditions.checkArgument(
        baseInputEntity instanceof RobuxSegmentInputEntity,
        RobuxSegmentInputEntity.class.getName() + " required, but "
        + baseInputEntity.getClass().getName() + " provided."
    );

    final RobuxSegmentInputEntity robuxSegmentEntity = (RobuxSegmentInputEntity) baseInputEntity;
    if (robuxSegmentEntity.isFromTombstone()) {
      return new RobuxTombstoneSegmentReader(robuxSegmentEntity);
    } else {
      return new RobuxSegmentReader(
          source,
          indexIO,
          inputRowSchema.getTimestampSpec(),
          inputRowSchema.getDimensionsSpec(),
          inputRowSchema.getColumnsFilter(),
          dimFilter,
          temporaryDirectory
      );
    }
  }
}

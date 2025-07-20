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

package org.apache.robux.sql.calcite.util.datasets;

import org.apache.robux.data.input.InputRow;
import org.apache.robux.data.input.InputRowSchema;
import org.apache.robux.data.input.impl.DimensionSchema;
import org.apache.robux.data.input.impl.MapInputRowParser;
import org.apache.robux.query.aggregation.AggregatorFactory;
import org.apache.robux.segment.IndexBuilder;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.segment.column.RowSignature.Builder;
import org.apache.robux.segment.incremental.IncrementalIndexSchema;
import org.apache.robux.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.LinearShardSpec;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class MapBasedTestDataset implements TestDataSet
{
  protected final String name;

  protected MapBasedTestDataset(String name)
  {
    this.name = name;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public final DataSegment makeSegment(final QueryableIndex index)
  {
    DataSegment segment = DataSegment.builder()
        .dataSource(name)
        .interval(index.getDataInterval())
        .version("1")
        .shardSpec(new LinearShardSpec(0))
        .size(0)
        .build();
    return segment;
  }

  @Override
  public final QueryableIndex makeIndex(File tmpDir)
  {
    return IndexBuilder
        .create()
        .tmpDir(new File(tmpDir, "idx"))
        .inputTmpDir(new File(tmpDir, "input"))
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(getIndexSchema())
        .rows(getRows())
        .buildMMappedIndex();
  }

  public IncrementalIndexSchema getIndexSchema()
  {
    return new IncrementalIndexSchema.Builder()
        .withMetrics(getMetrics().toArray(new AggregatorFactory[0]))
        .withDimensionsSpec(getInputRowSchema().getDimensionsSpec())
        .withRollup(false)
        .build();
  }

  public final Iterable<InputRow> getRows()
  {
    return getRawRows()
        .stream()
        .map(raw -> createRow(raw, getInputRowSchema()))
        .collect(Collectors.toList());
  }

  public static InputRow createRow(final Map<String, ?> map, InputRowSchema inputRowSchema)
  {
    return MapInputRowParser.parse(inputRowSchema, (Map<String, Object>) map);
  }

  public RowSignature getInputRowSignature()
  {
    Builder rsBuilder = RowSignature.builder();
    for (DimensionSchema dimensionSchema : getInputRowSchema().getDimensionsSpec().getDimensions()) {
      rsBuilder.add(dimensionSchema.getName(), dimensionSchema.getColumnType());
    }
    return rsBuilder.build();
  }

  public abstract InputRowSchema getInputRowSchema();

  public abstract List<Map<String, Object>> getRawRows();

  public abstract List<AggregatorFactory> getMetrics();
}

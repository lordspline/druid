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

package org.apache.robux.compressedbigdecimal.serde;

import org.apache.robux.compressedbigdecimal.CompressedBigDecimal;
import org.apache.robux.compressedbigdecimal.CompressedBigDecimalColumnPartSupplier;
import org.apache.robux.compressedbigdecimal.CompressedBigDecimalModule;
import org.apache.robux.compressedbigdecimal.CompressedBigDecimalObjectStrategy;
import org.apache.robux.compressedbigdecimal.Utils;
import org.apache.robux.data.input.InputRow;
import org.apache.robux.segment.IndexSpec;
import org.apache.robux.segment.column.ColumnBuilder;
import org.apache.robux.segment.data.ObjectStrategy;
import org.apache.robux.segment.serde.ComplexMetricExtractor;
import org.apache.robux.segment.serde.ComplexMetricSerde;
import org.apache.robux.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;

/**
 * ComplexMetricSerde that understands how to read and write scaled longs.
 */
public class CompressedBigDecimalMetricSerde extends ComplexMetricSerde
{

  private final CompressedBigDecimalObjectStrategy strategy = new CompressedBigDecimalObjectStrategy();

  /* (non-Javadoc)
   * @see ComplexMetricSerde#getTypeName()
   */
  @Override
  public String getTypeName()
  {
    return CompressedBigDecimalModule.COMPRESSED_BIG_DECIMAL;
  }

  @Override
  public ComplexMetricExtractor<CompressedBigDecimal> getExtractor()
  {
    return new ComplexMetricExtractor<>()
    {
      @Override
      public Class<CompressedBigDecimal> extractedClass()
      {
        return CompressedBigDecimal.class;
      }

      @Override
      public CompressedBigDecimal extractValue(InputRow inputRow, String metricName)
      {
        Object rawMetric = inputRow.getRaw(metricName);

        return Utils.objToCompressedBigDecimal(rawMetric);
      }
    };
  }

  /* (non-Javadoc)
   * @see ComplexMetricSerde#deserializeColumn(java.nio.ByteBuffer, org.apache.robux.segment.column.ColumnBuilder)
   */
  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    builder.setComplexColumnSupplier(
        CompressedBigDecimalColumnPartSupplier.fromByteBuffer(buffer, builder.getFileMapper())
    );
  }

  /* (non-Javadoc)
   * @see org.apache.robux.segment.serde.ComplexMetricSerde#getSerializer(org.apache.robux.segment.data.IOPeon,
   * java.lang.String)
   */
  @Override
  public CompressedBigDecimalLongColumnSerializer getSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String column,
      IndexSpec indexSpec
  )
  {
    return CompressedBigDecimalLongColumnSerializer.create(segmentWriteOutMedium, column);
  }

  /* (non-Javadoc)
   * @see ComplexMetricSerde#getObjectStrategy()
   */
  @Override
  public ObjectStrategy<CompressedBigDecimal> getObjectStrategy()
  {
    return strategy;
  }
}

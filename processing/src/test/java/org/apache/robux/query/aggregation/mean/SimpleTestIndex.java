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

package org.apache.robux.query.aggregation.mean;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.robux.data.input.InputRow;
import org.apache.robux.data.input.MapBasedInputRow;
import org.apache.robux.data.input.impl.NoopInputRowParser;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.query.aggregation.AggregationTestHelper;
import org.apache.robux.query.aggregation.AggregatorFactory;
import org.apache.robux.query.aggregation.CountAggregatorFactory;
import org.apache.robux.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;

public class SimpleTestIndex
{
  public static final int NUM_ROWS = 10;

  public static final String SINGLE_VALUE_DOUBLE_AS_STRING_DIM = "singleValueDoubleAsStringDim";
  public static final String MULTI_VALUE_DOUBLE_AS_STRING_DIM = "multiValueDoubleAsStringDim";

  public static final String DOUBLE_COL = "doubleCol";

  public static final List<String> DIMENSIONS = ImmutableList.of(
      SINGLE_VALUE_DOUBLE_AS_STRING_DIM,
      MULTI_VALUE_DOUBLE_AS_STRING_DIM
  );

  private static Supplier<IncrementalIndex> realtimeIndex = Suppliers.memoize(
      () -> makeRealtimeIndex()
  );

  private static Supplier<QueryableIndex> mmappedIndex = Suppliers.memoize(
      () -> TestIndex.persistAndMemoryMap(realtimeIndex.get())
  );


  public static IncrementalIndex getIncrementalTestIndex()
  {
    return realtimeIndex.get();
  }

  public static QueryableIndex getMMappedTestIndex()
  {
    return mmappedIndex.get();
  }

  private static IncrementalIndex makeRealtimeIndex()
  {
    try {
      List<InputRow> inputRows = Lists.newArrayListWithExpectedSize(NUM_ROWS);
      for (int i = 1; i <= NUM_ROWS; i++) {
        double doubleVal = i + 0.7d;
        String stringVal = String.valueOf(doubleVal);

        inputRows.add(new MapBasedInputRow(
            DateTime.now(DateTimeZone.UTC),
            DIMENSIONS,
            ImmutableMap.of(
                DOUBLE_COL, doubleVal,
                SINGLE_VALUE_DOUBLE_AS_STRING_DIM, stringVal,
                MULTI_VALUE_DOUBLE_AS_STRING_DIM, Lists.newArrayList(stringVal, null, stringVal)
            )
        ));
      }

      return AggregationTestHelper.createIncrementalIndex(
          inputRows.iterator(),
          new NoopInputRowParser(null),
          new AggregatorFactory[]{
              new CountAggregatorFactory("count"),
              new DoubleSumAggregatorFactory(DOUBLE_COL, DOUBLE_COL)
          },
          0,
          Granularities.NONE,
          100,
          false
      );
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}

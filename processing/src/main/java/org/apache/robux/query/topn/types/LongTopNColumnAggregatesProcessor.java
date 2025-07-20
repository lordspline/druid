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

package org.apache.robux.query.topn.types;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.robux.query.aggregation.Aggregator;
import org.apache.robux.query.topn.BaseTopNAlgorithm;
import org.apache.robux.query.topn.TopNQuery;
import org.apache.robux.segment.BaseLongColumnValueSelector;
import org.apache.robux.segment.Cursor;

import java.util.Map;
import java.util.function.Function;

public class LongTopNColumnAggregatesProcessor
    extends NullableNumericTopNColumnAggregatesProcessor<BaseLongColumnValueSelector>
{
  private Long2ObjectMap<Aggregator[]> aggregatesStore;

  public LongTopNColumnAggregatesProcessor(Function<Object, Object> converter)
  {
    super(converter);
  }

  @Override
  Aggregator[] getValueAggregators(TopNQuery query, BaseLongColumnValueSelector selector, Cursor cursor)
  {
    long key = selector.getLong();
    return aggregatesStore.computeIfAbsent(
        key,
        k -> BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs())
    );
  }

  @Override
  public void initAggregateStore()
  {
    nullValueAggregates = null;
    aggregatesStore = new Long2ObjectOpenHashMap<>();
  }

  @Override
  Map<?, Aggregator[]> getAggregatesStore()
  {
    return aggregatesStore;
  }

  @Override
  Object convertAggregatorStoreKeyToColumnValue(Object aggregatorStoreKey)
  {
    return converter.apply(aggregatorStoreKey);
  }
}

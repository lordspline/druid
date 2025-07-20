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

package org.apache.robux.query.aggregation.datasketches.hll.sql;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.robux.java.util.common.StringEncoding;
import org.apache.robux.query.aggregation.AggregatorFactory;
import org.apache.robux.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.robux.sql.calcite.aggregation.Aggregation;
import org.apache.robux.sql.calcite.aggregation.SqlAggregator;
import org.apache.robux.sql.calcite.expression.OperatorConversions;

import java.util.Collections;

/**
 * Like {@link HllSketchApproxCountDistinctSqlAggregator}, but uses {@link StringEncoding#UTF8} instead of
 * {@link org.apache.robux.query.aggregation.datasketches.hll.HllSketchAggregatorFactory#DEFAULT_STRING_ENCODING}.
 *
 * Currently undocumented. Only accepts strings, not sketches. The purpose of this function is to allow us to experiment
 * with UTF-8-based HLL counting while we figure out how the API should work.
 *
 * See https://github.com/apache/robux/pull/11201 for details.
 */
public class HllSketchApproxCountDistinctUtf8SqlAggregator
    extends HllSketchBaseSqlAggregator
    implements SqlAggregator
{
  public static final String NAME = "APPROX_COUNT_DISTINCT_DS_HLL_UTF8";
  private static final SqlAggFunction FUNCTION_INSTANCE =
      OperatorConversions.aggregatorBuilder(NAME)
                         .operandNames("column", "lgK", "tgtHllType")
                         .operandTypes(SqlTypeFamily.STRING, SqlTypeFamily.NUMERIC, SqlTypeFamily.STRING)
                         .operandTypeInference(InferTypes.VARCHAR_1024)
                         .requiredOperandCount(1)
                         .literalOperands(1, 2)
                         .returnTypeNonNull(SqlTypeName.BIGINT)
                         .functionCategory(SqlFunctionCategory.NUMERIC)
                         .build();

  public HllSketchApproxCountDistinctUtf8SqlAggregator()
  {
    super(true, StringEncoding.UTF8);
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Override
  protected Aggregation toAggregation(
      String name,
      boolean finalizeAggregations,
      AggregatorFactory aggregatorFactory
  )
  {
    return Aggregation.create(
        Collections.singletonList(aggregatorFactory),
        finalizeAggregations ? new FinalizingFieldAccessPostAggregator(name, aggregatorFactory.getName()) : null
    );
  }
}

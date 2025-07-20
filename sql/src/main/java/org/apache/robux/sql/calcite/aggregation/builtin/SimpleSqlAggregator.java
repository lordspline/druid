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

package org.apache.robux.sql.calcite.aggregation.builtin;

import com.google.common.collect.Iterables;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.robux.error.RobuxException;
import org.apache.robux.error.InvalidSqlInput;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.sql.calcite.aggregation.Aggregation;
import org.apache.robux.sql.calcite.aggregation.Aggregations;
import org.apache.robux.sql.calcite.aggregation.SqlAggregator;
import org.apache.robux.sql.calcite.expression.RobuxExpression;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.InputAccessor;
import org.apache.robux.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Abstraction for single column, single argument simple aggregators like sum, avg, min, max that:
 *
 * 1) Can take direct field accesses or expressions as inputs.
 * 2) Cannot implicitly cast strings to numbers when using a direct field access.
 *
 * @see Aggregations#getArgumentsForSimpleAggregator for details on these requirements
 */
public abstract class SimpleSqlAggregator implements SqlAggregator
{
  public static RobuxException badTypeException(String columnName, String agg, ColumnType type)
  {
    return InvalidSqlInput.exception("Aggregation [%s] does not support type [%s], column [%s]", agg, type, columnName);
  }

  @Nullable
  @Override
  public Aggregation toRobuxAggregation(
      final PlannerContext plannerContext,
      final VirtualColumnRegistry virtualColumnRegistry,
      final String name,
      final AggregateCall aggregateCall,
      final InputAccessor inputAccessor,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    if (aggregateCall.isDistinct()) {
      return null;
    }

    final List<RobuxExpression> arguments = Aggregations.getArgumentsForSimpleAggregator(
        plannerContext,
        aggregateCall,
        inputAccessor
    );

    if (arguments == null) {
      return null;
    }

    final RobuxExpression arg = Iterables.getOnlyElement(arguments);
    final ExprMacroTable macroTable = plannerContext.getPlannerToolbox().exprMacroTable();

    final String fieldName;

    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      // sharing is caring, make a virtual column to maximize re-use
      fieldName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(arg, aggregateCall.getType());
    }

    return getAggregation(name, aggregateCall, macroTable, fieldName);
  }

  abstract Aggregation getAggregation(
      String name,
      AggregateCall aggregateCall,
      ExprMacroTable macroTable,
      String fieldName
  );
}

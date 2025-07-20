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

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.robux.query.aggregation.FilteredAggregatorFactory;
import org.apache.robux.query.filter.NotDimFilter;
import org.apache.robux.query.filter.NullFilter;
import org.apache.robux.query.filter.SelectorDimFilter;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.sql.calcite.aggregation.Aggregation;
import org.apache.robux.sql.calcite.aggregation.SqlAggregator;
import org.apache.robux.sql.calcite.expression.RobuxExpression;
import org.apache.robux.sql.calcite.expression.Expressions;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.InputAccessor;
import org.apache.robux.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class BitwiseSqlAggregator implements SqlAggregator
{
  public enum Op
  {
    AND {
      @Override
      SqlAggFunction getCalciteFunction()
      {
        return SqlStdOperatorTable.BIT_AND;
      }

      @Override
      String getRobuxFunction()
      {
        return "bitwiseAnd";
      }
    },
    OR {
      @Override
      SqlAggFunction getCalciteFunction()
      {
        return SqlStdOperatorTable.BIT_OR;
      }

      @Override
      String getRobuxFunction()
      {
        return "bitwiseOr";
      }
    },
    XOR {
      @Override
      SqlAggFunction getCalciteFunction()
      {
        return SqlStdOperatorTable.BIT_XOR;
      }

      @Override
      String getRobuxFunction()
      {
        return "bitwiseXor";
      }
    };

    abstract SqlAggFunction getCalciteFunction();
    abstract String getRobuxFunction();
  };

  private final Op op;

  public BitwiseSqlAggregator(Op op)
  {
    this.op = op;
  }

  @Override
  public SqlAggFunction calciteFunction()
  {
    return op.getCalciteFunction();
  }

  @Nullable
  @Override
  public Aggregation toRobuxAggregation(
      PlannerContext plannerContext,
      VirtualColumnRegistry virtualColumnRegistry,
      String name,
      AggregateCall aggregateCall,
      InputAccessor inputAccessor,
      List<Aggregation> existingAggregations,
      boolean finalizeAggregations
  )
  {
    final List<RobuxExpression> arguments = aggregateCall
        .getArgList()
        .stream()
        .map(i -> inputAccessor.getField(i))
        .map(rexNode -> Expressions.toRobuxExpression(plannerContext, inputAccessor.getInputRowSignature(), rexNode))
        .collect(Collectors.toList());

    if (arguments.stream().anyMatch(Objects::isNull)) {
      return null;
    }

    final RobuxExpression arg = arguments.get(0);
    final ExprMacroTable macroTable = plannerContext.getPlannerToolbox().exprMacroTable();

    final String fieldName;
    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      fieldName = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(arg, ColumnType.LONG);
    }

    return Aggregation.create(
        new FilteredAggregatorFactory(
            new ExpressionLambdaAggregatorFactory(
                name,
                ImmutableSet.of(fieldName),
                null,
                "0",
                null,
                null,
                false,
                false,
                StringUtils.format("%s(\"__acc\", \"%s\")", op.getRobuxFunction(), fieldName),
                null,
                null,
                null,
                null,
                macroTable
            ),
            new NotDimFilter(
                plannerContext.isUseBoundsAndSelectors()
                ? new SelectorDimFilter(fieldName, null, null)
                : NullFilter.forColumn(fieldName)
            )
        )
    );
  }
}

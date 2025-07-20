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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.CastedLiteralOperandTypeCheckers;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;
import org.apache.robux.java.util.common.HumanReadableBytes;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.math.expr.ExpressionType;
import org.apache.robux.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.robux.segment.VirtualColumn;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.sql.calcite.aggregation.Aggregation;
import org.apache.robux.sql.calcite.aggregation.NativelySupportsDistinct;
import org.apache.robux.sql.calcite.aggregation.SqlAggregator;
import org.apache.robux.sql.calcite.expression.RobuxExpression;
import org.apache.robux.sql.calcite.expression.Expressions;
import org.apache.robux.sql.calcite.planner.Calcites;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.InputAccessor;
import org.apache.robux.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public class ArrayConcatSqlAggregator implements SqlAggregator
{
  private static final String NAME = "ARRAY_CONCAT_AGG";
  private static final SqlAggFunction FUNCTION = new ArrayConcatAggFunction();

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION;
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
    final List<RexNode> arguments = inputAccessor.getFields(aggregateCall.getArgList());

    Integer maxSizeBytes = null;
    if (arguments.size() > 1) {
      RexNode maxBytes = arguments.get(1);
      if (!maxBytes.isA(SqlKind.LITERAL)) {
        // maxBytes must be a literal
        return null;
      }
      maxSizeBytes = ((Number) RexLiteral.value(maxBytes)).intValue();
    }
    final RobuxExpression arg = Expressions.toRobuxExpression(plannerContext, inputAccessor.getInputRowSignature(), arguments.get(0));
    final ExprMacroTable macroTable = plannerContext.getPlannerToolbox().exprMacroTable();

    final String fieldName;
    final ColumnType robuxType = Calcites.getValueTypeForRelDataTypeFull(aggregateCall.getType());
    if (robuxType == null || !robuxType.isArray()) {
      // must be an array
      return null;
    }
    final String initialvalue = ExpressionType.fromColumnTypeStrict(robuxType).asTypeString() + "[]";
    if (arg.isDirectColumnAccess()) {
      fieldName = arg.getDirectColumn();
    } else {
      VirtualColumn vc = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(plannerContext, arg, robuxType);
      fieldName = vc.getOutputName();
    }

    if (aggregateCall.isDistinct()) {
      return Aggregation.create(
          new ExpressionLambdaAggregatorFactory(
              name,
              ImmutableSet.of(fieldName),
              null,
              initialvalue,
              null,
              true,
              false,
              false,
              StringUtils.format("array_set_add_all(\"__acc\", \"%s\")", fieldName),
              StringUtils.format("array_set_add_all(\"__acc\", \"%s\")", name),
              null,
              null,
              maxSizeBytes != null ? new HumanReadableBytes(maxSizeBytes) : null,
              macroTable
          )
      );
    } else {
      return Aggregation.create(
          new ExpressionLambdaAggregatorFactory(
              name,
              ImmutableSet.of(fieldName),
              null,
              initialvalue,
              null,
              true,
              false,
              false,
              StringUtils.format("array_concat(\"__acc\", \"%s\")", fieldName),
              StringUtils.format("array_concat(\"__acc\", \"%s\")", name),
              null,
              null,
              maxSizeBytes != null ? new HumanReadableBytes(maxSizeBytes) : null,
              macroTable
          )
      );
    }
  }

  @NativelySupportsDistinct
  private static class ArrayConcatAggFunction extends SqlAggFunction
  {
    ArrayConcatAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.ARG0,
          InferTypes.ANY_NULLABLE,
          OperandTypes.or(
              OperandTypes.ARRAY,
              OperandTypes.sequence(
                  StringUtils.format("'%s(expr, maxSizeBytes)'", NAME),
                  OperandTypes.ARRAY,
                  CastedLiteralOperandTypeCheckers.POSITIVE_INTEGER_LITERAL
              )
          ),
          SqlFunctionCategory.USER_DEFINED_FUNCTION,
          false,
          false,
          Optionality.IGNORED
      );
    }
  }
}

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

package org.apache.robux.sql.calcite.expression;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.planner.Calcites;
import org.apache.robux.sql.calcite.planner.PlannerContext;

import javax.annotation.Nullable;

/**
 * Conversion for SQL operators that map 1-1 onto native functions.
 */
public class DirectOperatorConversion implements SqlOperatorConversion
{
  private final SqlOperator operator;
  private final String robuxFunctionName;

  public DirectOperatorConversion(final SqlOperator operator, final String robuxFunctionName)
  {
    this.operator = operator;
    this.robuxFunctionName = robuxFunctionName;
  }

  @Override
  public final SqlOperator calciteOperator()
  {
    return operator;
  }

  public final String getRobuxFunctionName()
  {
    return robuxFunctionName;
  }

  @Override
  public RobuxExpression toRobuxExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    return OperatorConversions.convertDirectCall(
        plannerContext,
        rowSignature,
        rexNode,
        robuxFunctionName
    );
  }

  @Nullable
  @Override
  public RobuxExpression toRobuxExpressionWithPostAggOperands(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode,
      PostAggregatorVisitor postAggregatorVisitor
  )
  {
    return OperatorConversions.convertCallWithPostAggOperands(
        plannerContext,
        rowSignature,
        rexNode,
        operands -> RobuxExpression.ofFunctionCall(
            Calcites.getColumnTypeForRelDataType(rexNode.getType()),
            robuxFunctionName,
            operands
        ),
        postAggregatorVisitor
    );
  }
}

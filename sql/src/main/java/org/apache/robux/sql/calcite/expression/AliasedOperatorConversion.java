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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.robux.query.aggregation.PostAggregator;
import org.apache.robux.query.filter.DimFilter;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;

public class AliasedOperatorConversion implements SqlOperatorConversion
{
  private final SqlOperatorConversion baseConversion;
  private final SqlOperator operator;

  public AliasedOperatorConversion(final SqlOperatorConversion baseConversion, final String name)
  {
    final SqlFunction baseFunction = (SqlFunction) baseConversion.calciteOperator();

    this.baseConversion = baseConversion;
    this.operator = new SqlFunction(
        name,
        baseFunction.getKind(),
        baseFunction.getReturnTypeInference(),
        baseFunction.getOperandTypeInference(),
        baseFunction.getOperandTypeChecker(),
        baseFunction.getFunctionType()
    );
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Override
  public RobuxExpression toRobuxExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    return baseConversion.toRobuxExpression(plannerContext, rowSignature, rexNode);
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
    return baseConversion.toRobuxExpression(plannerContext, rowSignature, rexNode);
  }

  @Nullable
  @Override
  public DimFilter toRobuxFilter(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      RexNode rexNode
  )
  {
    return baseConversion.toRobuxFilter(plannerContext, rowSignature, virtualColumnRegistry, rexNode);
  }

  @Nullable
  @Override
  public PostAggregator toPostAggregator(
      PlannerContext plannerContext,
      RowSignature querySignature,
      RexNode rexNode,
      PostAggregatorVisitor postAggregatorVisitor
  )
  {
    return baseConversion.toPostAggregator(plannerContext, querySignature, rexNode, postAggregatorVisitor);
  }
}

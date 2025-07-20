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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.robux.query.aggregation.post.ExpressionPostAggregator;
import org.apache.robux.sql.calcite.aggregation.Aggregation;
import org.apache.robux.sql.calcite.aggregation.SqlAggregator;
import org.apache.robux.sql.calcite.expression.RobuxExpression;
import org.apache.robux.sql.calcite.expression.Expressions;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.InputAccessor;
import org.apache.robux.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Calcite 1.35 introduces an aggrgate function LITERAL_AGG that returns constant value regardless
 * of how many rows are in the group. This also introduced a change to subquery
 * remove rule as a part of https://issues.apache.org/jira/browse/CALCITE-4334
 *
 * In this case a useless literal dimension is replaced with a post agg which makes queries performant
 * This class supports the use of LITERAL_AGG for Robux queries
 *
 */
public class LiteralSqlAggregator implements SqlAggregator
{
  @Override
  public SqlAggFunction calciteFunction()
  {
    return SqlInternalOperators.LITERAL_AGG;
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
    if (aggregateCall.rexList.size() == 0) {
      return null;
    }
    final RexNode literal = aggregateCall.rexList.get(0);
    final RobuxExpression expr = Expressions.toRobuxExpression(plannerContext, inputAccessor.getInputRowSignature(), literal);

    if (expr == null) {
      return null;
    }

    return Aggregation.create(
        ImmutableList.of(),
        new ExpressionPostAggregator(name, expr.getExpression(), null, expr.getRobuxType(), plannerContext.getExprMacroTable())
    );
  }
}

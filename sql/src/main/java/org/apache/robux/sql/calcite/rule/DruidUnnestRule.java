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

package org.apache.robux.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.robux.math.expr.Expr;
import org.apache.robux.math.expr.ExprEval;
import org.apache.robux.math.expr.InputBindings;
import org.apache.robux.query.InlineDataSource;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.expression.RobuxExpression;
import org.apache.robux.sql.calcite.expression.Expressions;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.RobuxQueryRel;
import org.apache.robux.sql.calcite.rel.RobuxUnnestRel;
import org.apache.robux.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class creates the rule to abide by for creating unnest (internally uncollect) in Calcite.
 * Typically, Calcite plans the *unnest* part of the query involving a table such as
 * <pre>
 * SELECT * from numFoo, unnest(dim3)
 * </pre>
 * or even a standalone unnest query such as
 * <pre>
 * SELECT * from unnest(ARRAY[1,2,3]) in the following way:
 *   78:Uncollect(subset=[rel#79:Subset#3.NONE.[]])
 *     76:LogicalProject(subset=[rel#77:Subset#2.NONE.[]], EXPR$0=[MV_TO_ARRAY($cor0.dim3)])
 *       7:LogicalValues(subset=[rel#75:Subset#1.NONE.[0]], tuples=[[{ 0 }]])
 * </pre>
 * Calcite tackles plans bottom up. Therefore,
 * {@link RobuxLogicalValuesRule} converts the LogicalValues part into a leaf level {@link RobuxQueryRel}
 * thereby creating the following subtree in the call tree
 *
 * <pre>
 * Uncollect
 *  \
 *  LogicalProject
 *   \
 *   RobuxQueryRel
 * </pre>
 *
 * This forms the premise of this rule. The goal is to transform the above-mentioned structure in the tree
 * with a new rel {@link RobuxUnnestRel} which shall be created here.
 */
public class RobuxUnnestRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public RobuxUnnestRule(PlannerContext plannerContext)
  {
    super(
        operand(
            Uncollect.class,
            operand(Project.class, operand(Values.class, none()))
        )
    );
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Project projectRel = call.rel(1);
    final Values valuesRel = call.rel(2);

    // Project must be a single field on top of a single row, and not refer to any bits of the input.
    // (The single row is a dummy row. We expect the Project expr to be a constant or a correlated field access.)
    return projectRel.getProjects().size() == 1
           && valuesRel.getTuples().size() == 1
           && RelOptUtil.InputFinder.bits(projectRel.getProjects(), null).isEmpty();
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Uncollect uncollectRel = call.rel(0);
    final Project projectRel = call.rel(1);

    final RexNode exprToUnnest = projectRel.getProjects().get(0);
    if (RexUtil.isConstant(exprToUnnest)) {
      // Constant expression: transform to RobuxQueryRel on an inline datasource.
      final InlineDataSource inlineDataSource = toInlineDataSource(
          uncollectRel,
          exprToUnnest,
          plannerContext
      );

      if (inlineDataSource != null) {
        call.transformTo(
            RobuxQueryRel.scanConstantRel(
                uncollectRel,
                inlineDataSource,
                plannerContext
            )
        );
      }
    } else {
      // Transform to RobuxUnnestRel, a holder for an unnest of a correlated variable.
      call.transformTo(
          RobuxUnnestRel.create(
              uncollectRel.getCluster(),
              uncollectRel.getTraitSet(),
              exprToUnnest,
              plannerContext
          )
      );
    }
  }

  @Nullable
  private static InlineDataSource toInlineDataSource(
      final Uncollect uncollectRel,
      final RexNode projectExpr,
      final PlannerContext plannerContext
  )
  {
    final RobuxExpression expression = Expressions.toRobuxExpression(
        plannerContext,
        RowSignature.empty(),
        projectExpr
    );

    if (expression == null) {
      return null;
    }

    // Evaluate the expression. It's a constant, so no bindings are needed.
    final Expr parsedExpression = plannerContext.parseExpression(expression.getExpression());
    final ExprEval<?> eval = parsedExpression.eval(InputBindings.nilBindings());
    final List<Object[]> rows = new ArrayList<>();

    if (eval.isArray()) {
      final Object[] evalArray = eval.asArray();
      if (evalArray != null) {
        for (Object o : evalArray) {
          rows.add(new Object[]{o});
        }
      }
    } else {
      rows.add(new Object[]{eval.valueOrDefault()});
    }

    // Transform to inline datasource.
    final RowSignature rowSignature = RowSignatures.fromRelDataType(
        uncollectRel.getRowType().getFieldNames(),
        uncollectRel.getRowType()
    );

    return InlineDataSource.fromIterable(rows, rowSignature);
  }
}

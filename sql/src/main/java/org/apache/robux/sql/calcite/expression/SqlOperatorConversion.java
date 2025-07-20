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
import org.apache.robux.query.aggregation.PostAggregator;
import org.apache.robux.query.filter.DimFilter;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;

public interface SqlOperatorConversion
{
  /**
   * Returns the SQL operator corresponding to this function. Should be a singleton.
   *
   * @return operator
   */
  SqlOperator calciteOperator();

  /**
   * Translate a Calcite {@code RexNode} to a Robux expression.
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNode        expression meant to be applied on top of the rows
   *
   * @return Robux expression, or null if translation is not possible
   *
   * @see Expressions#toRobuxExpression(PlannerContext, RowSignature, RexNode)
   */
  @Nullable
  RobuxExpression toRobuxExpression(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode
  );

  /**
   * Translate a Calcite {@code RexNode} to a Robux expression, with the possibility of having postagg operands.
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNode        expression meant to be applied on top of the rows
   * @param postAggregatorVisitor visitor that manages postagg names and tracks postaggs that were created as
   *                              by the translation
   *
   * @return Robux expression, or null if translation is not possible
   *
   * @see Expressions#toRobuxExpression(PlannerContext, RowSignature, RexNode)
   */
  @Nullable
  default RobuxExpression toRobuxExpressionWithPostAggOperands(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode,
      PostAggregatorVisitor postAggregatorVisitor
  )
  {
    return toRobuxExpression(plannerContext, rowSignature, rexNode);
  }

  /**
   * Returns a Robux filter corresponding to a Calcite {@code RexNode} used as a filter condition.
   *
   * @param plannerContext        SQL planner context
   * @param rowSignature          input row signature
   * @param virtualColumnRegistry re-usable virtual column references
   * @param rexNode               filter expression rex node
   *
   * @return filter, or null if the call cannot be translated to a filter
   */
  @Nullable
  default DimFilter toRobuxFilter(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      RexNode rexNode
  )
  {
    return null;
  }

  /**
   * Returns a Robux PostAggregator corresponding to a Calcite {@link RexNode} used to transform a row after
   * aggregation has occurred.
   *
   * @param plannerContext   SQL planner context
   * @param querySignature   signature of the rows to be extracted from
   * @param rexNode          expression meant to be applied on top of the rows
   *
   * @param postAggregatorVisitor visitor that manages postagg names and tracks postaggs that were created
   *                              by the translation
   * @return filter, or null if the call cannot be translated
   */
  @Nullable
  default PostAggregator toPostAggregator(
      PlannerContext plannerContext,
      RowSignature querySignature,
      RexNode rexNode,
      PostAggregatorVisitor postAggregatorVisitor
  )
  {
    return null;
  }
}

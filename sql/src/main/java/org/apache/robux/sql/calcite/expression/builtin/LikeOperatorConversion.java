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

package org.apache.robux.sql.calcite.expression.builtin;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.robux.query.filter.DimFilter;
import org.apache.robux.query.filter.LikeDimFilter;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.expression.DirectOperatorConversion;
import org.apache.robux.sql.calcite.expression.RobuxExpression;
import org.apache.robux.sql.calcite.expression.Expressions;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public class LikeOperatorConversion extends DirectOperatorConversion
{
  private static final SqlOperator SQL_FUNCTION = SqlStdOperatorTable.LIKE;

  public LikeOperatorConversion()
  {
    super(SQL_FUNCTION, "like");
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
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final RobuxExpression robuxExpression = Expressions.toRobuxExpression(
        plannerContext,
        rowSignature,
        operands.get(0)
    );
    if (robuxExpression == null) {
      return null;
    }

    if (robuxExpression.isSimpleExtraction()) {
      return new LikeDimFilter(
          robuxExpression.getSimpleExtraction().getColumn(),
          RexLiteral.stringValue(operands.get(1)),
          operands.size() > 2 ? RexLiteral.stringValue(operands.get(2)) : null,
          robuxExpression.getSimpleExtraction().getExtractionFn()
      );
    } else if (virtualColumnRegistry != null) {
      String v = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          robuxExpression,
          operands.get(0).getType()
      );

      return new LikeDimFilter(
          v,
          RexLiteral.stringValue(operands.get(1)),
          operands.size() > 2 ? RexLiteral.stringValue(operands.get(2)) : null,
          null
      );
    } else {
      return null;
    }
  }
}

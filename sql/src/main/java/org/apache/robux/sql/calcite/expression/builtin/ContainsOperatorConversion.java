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
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.query.expression.CaseInsensitiveContainsExprMacro;
import org.apache.robux.query.expression.ContainsExprMacro;
import org.apache.robux.query.filter.DimFilter;
import org.apache.robux.query.filter.SearchQueryDimFilter;
import org.apache.robux.query.search.ContainsSearchQuerySpec;
import org.apache.robux.query.search.SearchQuerySpec;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.expression.DirectOperatorConversion;
import org.apache.robux.sql.calcite.expression.RobuxExpression;
import org.apache.robux.sql.calcite.expression.Expressions;
import org.apache.robux.sql.calcite.expression.OperatorConversions;
import org.apache.robux.sql.calcite.expression.SqlOperatorConversion;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Register {@code contains_string} and {@code icontains_string} functions with calcite that internally
 * translate these functions into {@link SearchQueryDimFilter} with {@link ContainsSearchQuerySpec} as
 * search query spec.
 */
public class ContainsOperatorConversion extends DirectOperatorConversion
{
  private final boolean caseSensitive;

  private ContainsOperatorConversion(
      final SqlFunction sqlFunction,
      final boolean caseSensitive
  )
  {
    super(sqlFunction, StringUtils.toLowerCase(sqlFunction.getName()));
    this.caseSensitive = caseSensitive;
  }

  public static SqlOperatorConversion caseSensitive()
  {
    final SqlFunction sqlFunction = createSqlFunction(ContainsExprMacro.FN_NAME);
    return new ContainsOperatorConversion(sqlFunction, true);
  }

  public static SqlOperatorConversion caseInsensitive()
  {
    final SqlFunction sqlFunction = createSqlFunction(CaseInsensitiveContainsExprMacro.FN_NAME);
    return new ContainsOperatorConversion(sqlFunction, false);
  }

  private static SqlFunction createSqlFunction(final String functionName)
  {
    return OperatorConversions
        .operatorBuilder(StringUtils.toUpperCase(functionName))
        .operandTypes(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER)
        .requiredOperandCount(2)
        .literalOperands(1)
        .returnTypeNullable(SqlTypeName.BOOLEAN)
        .functionCategory(SqlFunctionCategory.STRING)
        .build();
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

    final String search = RexLiteral.stringValue(operands.get(1));
    final SearchQuerySpec spec = new ContainsSearchQuerySpec(search, caseSensitive);

    if (robuxExpression.isSimpleExtraction()) {
      return new SearchQueryDimFilter(
          robuxExpression.getSimpleExtraction().getColumn(),
          spec,
          robuxExpression.getSimpleExtraction().getExtractionFn(),
          null
      );
    } else if (virtualColumnRegistry != null) {
      String v = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
          robuxExpression,
          operands.get(0).getType()
      );

      return new SearchQueryDimFilter(v, spec, null, null);
    } else {
      return null;
    }
  }
}

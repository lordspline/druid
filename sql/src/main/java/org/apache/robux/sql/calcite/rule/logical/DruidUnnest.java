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

package org.apache.robux.sql.calcite.rule.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.robux.query.DataSource;
import org.apache.robux.query.UnnestDataSource;
import org.apache.robux.query.filter.DimFilter;
import org.apache.robux.segment.VirtualColumn;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.expression.RobuxExpression;
import org.apache.robux.sql.calcite.expression.Expressions;
import org.apache.robux.sql.calcite.filtration.Filtration;
import org.apache.robux.sql.calcite.planner.Calcites;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.planner.querygen.SourceDescProducer;
import org.apache.robux.sql.calcite.rel.RobuxJoinQueryRel;
import org.apache.robux.sql.calcite.rel.logical.RobuxLogicalNode;

import java.util.List;

public class RobuxUnnest extends Unnest implements RobuxLogicalNode, SourceDescProducer
{
  protected RobuxUnnest(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode unnestExpr,
      RelDataTypeField unnestFieldType, RexNode condition)
  {
    super(cluster, traits, input, unnestExpr, unnestFieldType, condition);
  }

  @Override
  protected RelNode copy(RelTraitSet traitSet, RelNode input)
  {
    return new RobuxUnnest(getCluster(), traitSet, input, unnestExpr, unnestFieldType, filter);
  }

  @Override
  public SourceDesc getSourceDesc(PlannerContext plannerContext, List<SourceDesc> sources)
  {
    SourceDesc inputDesc = sources.get(0);

    RowSignature outputRowSignature = computeRowOutputSignature(inputDesc);

    RowSignature filterRowSignature = RowSignature.builder().add(
        outputRowSignature.getColumnName(outputRowSignature.size() - 1),
        outputRowSignature.getColumnType(outputRowSignature.size() - 1).get()
    ).build();

    VirtualColumn virtualColumn = buildUnnestVirtualColumn(
        plannerContext,
        inputDesc,
        filterRowSignature.getColumnName(0)
    );

    DimFilter dimFilter = buildDimFilter(plannerContext, filterRowSignature);
    DataSource dataSource = UnnestDataSource.create(inputDesc.dataSource, virtualColumn, dimFilter);
    return new SourceDesc(dataSource, outputRowSignature);
  }

  private DimFilter buildDimFilter(PlannerContext plannerContext, RowSignature filterRowSignature)
  {
    if (filter == null) {
      return null;
    }
    DimFilter dimFilter = Expressions.toFilter(
        plannerContext,
        filterRowSignature,
        null,
        filter
    );
    return Filtration.create(dimFilter).optimizeFilterOnly(filterRowSignature).getDimFilter();
  }

  private VirtualColumn buildUnnestVirtualColumn(PlannerContext plannerContext, SourceDesc inputDesc, String columnName)
  {
    final RobuxExpression expressionToUnnest = Expressions.toRobuxExpression(
        plannerContext,
        inputDesc.rowSignature,
        unnestExpr
    );

    VirtualColumn virtualColumn = expressionToUnnest.toVirtualColumn(
        columnName,
        Calcites.getColumnTypeForRelDataType(
            unnestExpr.getType()
        ),
        plannerContext.getExpressionParser()
    );
    return virtualColumn;
  }

  private RowSignature computeRowOutputSignature(SourceDesc inputDesc)
  {
    return RobuxJoinQueryRel.computeJoinRowSignature(
        inputDesc.rowSignature,
        RowSignature.builder().add(
            "unnest",
            Calcites.getColumnTypeForRelDataType(getUnnestedType())
        ).build(),
        RobuxJoinQueryRel.findExistingJoinPrefixes(inputDesc.dataSource)
    ).rhs;
  }

  private RelDataType getUnnestedType()
  {
    return unnestFieldType.getType();
  }
}

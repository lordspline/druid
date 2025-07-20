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

package org.apache.robux.sql.calcite.rel.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.robux.query.InlineDataSource;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.planner.querygen.SourceDescProducer;
import org.apache.robux.sql.calcite.rel.CostEstimates;
import org.apache.robux.sql.calcite.rule.RobuxLogicalValuesRule;
import org.apache.robux.sql.calcite.table.InlineTable;
import org.apache.robux.sql.calcite.table.RowSignatures;

import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link RobuxLogicalNode} convention node for {@link LogicalValues} plan node.
 */
public class RobuxValues extends LogicalValues implements RobuxLogicalNode, SourceDescProducer
{

  private InlineTable inlineTable;

  public RobuxValues(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples)
  {
    super(cluster, traitSet, rowType, tuples);
    assert getConvention() instanceof RobuxLogicalConvention;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs)
  {
    return new RobuxValues(getCluster(), traitSet, getRowType(), tuples);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeCost(CostEstimates.COST_BASE, 0, 0);
  }

  @Override
  public SourceDesc getSourceDesc(PlannerContext plannerContext, List<SourceDesc> sources)
  {
    if (inlineTable == null) {
      inlineTable = buildInlineTable(plannerContext);
    }
    return new SourceDesc(inlineTable.getDataSource(), inlineTable.getRowSignature());
  }

  private InlineTable buildInlineTable(PlannerContext plannerContext)
  {

    final List<ImmutableList<RexLiteral>> tuples = getTuples();
    final List<Object[]> objectTuples = tuples
        .stream()
        .map(
            tuple -> tuple
                .stream()
                .map(v -> RobuxLogicalValuesRule.getValueFromLiteral(v, plannerContext))
                .collect(Collectors.toList())
                .toArray(new Object[0])
        )
        .collect(Collectors.toList());
    RowSignature rowSignature = RowSignatures.fromRelDataType(
        getRowType().getFieldNames(),
        getRowType()
    );
    InlineTable inlineTable = new InlineTable(InlineDataSource.fromIterable(objectTuples, rowSignature));

    return inlineTable;
  }
}

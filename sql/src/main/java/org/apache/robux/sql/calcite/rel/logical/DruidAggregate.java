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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.CostEstimates;

import java.util.List;

/**
 * {@link RobuxLogicalNode} convention node for {@link Aggregate} plan node.
 */
public class RobuxAggregate extends Aggregate implements RobuxLogicalNode
{
  private final PlannerContext plannerContext;

  public RobuxAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls,
      PlannerContext plannerContext
  )
  {
    super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    assert getConvention() instanceof RobuxLogicalConvention;
    this.plannerContext = plannerContext;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
  {
    double rowCount = mq.getRowCount(this);
    double cost = CostEstimates.COST_DIMENSION * getGroupSet().size();
    for (AggregateCall aggregateCall : getAggCallList()) {
      if (aggregateCall.hasFilter()) {
        cost += CostEstimates.COST_AGGREGATION * CostEstimates.MULTIPLIER_FILTER;
      } else {
        cost += CostEstimates.COST_AGGREGATION;
      }
    }
    if (!plannerContext.getPlannerConfig().isUseApproximateCountDistinct() &&
        getAggCallList().stream().anyMatch(AggregateCall::isDistinct)) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return planner.getCostFactory().makeCost(rowCount, cost, 0);
  }

  @Override
  public final Aggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls
  )
  {
    return new RobuxAggregate(getCluster(), traitSet, input, groupSet, groupSets, aggCalls, plannerContext);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw)
  {
    return super.explainTerms(pw).item("robux", "logical");
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq)
  {
    return mq.getRowCount(this);
  }
}

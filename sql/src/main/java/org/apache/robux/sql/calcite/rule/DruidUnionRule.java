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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.RobuxRel;
import org.apache.robux.sql.calcite.rel.RobuxUnionRel;
import org.apache.robux.sql.calcite.run.EngineFeature;

import java.util.List;

/**
 * Rule that creates a {@link RobuxUnionRel} from some {@link RobuxRel} inputs.
 */
public class RobuxUnionRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public RobuxUnionRule(PlannerContext plannerContext)
  {
    super(
        operand(
            Union.class,
            operand(RobuxRel.class, none()),
            operand(RobuxRel.class, none())
        )
    );
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    if (plannerContext != null && !plannerContext.featureAvailable(EngineFeature.ALLOW_TOP_LEVEL_UNION_ALL)) {
      plannerContext.setPlanningError("Queries cannot be planned using top level union all");
      return false;
    }
    // Make RobuxUnionRule and RobuxUnionDataSourceRule mutually exclusive.
    final Union unionRel = call.rel(0);
    final RobuxRel<?> firstRobuxRel = call.rel(1);
    final RobuxRel<?> secondRobuxRel = call.rel(2);
    return !RobuxUnionDataSourceRule.isCompatible(unionRel, firstRobuxRel, secondRobuxRel, null);
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final RobuxRel<?> someRobuxRel = call.rel(1);
    final List<RelNode> inputs = unionRel.getInputs();

    // Can only do UNION ALL.
    if (unionRel.all) {
      call.transformTo(
          RobuxUnionRel.create(
              someRobuxRel.getPlannerContext(),
              unionRel.getRowType(),
              inputs,
              -1
          )
      );
    } else {
      plannerContext.setPlanningError("SQL requires 'UNION' but only 'UNION ALL' is supported.");
    }
  }
}

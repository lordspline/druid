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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.logical.RobuxLogicalConvention;

import java.util.ArrayList;
import java.util.List;


public class RobuxLogicalRules
{
  private final PlannerContext plannerContext;

  public RobuxLogicalRules(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  public List<RelOptRule> rules()
  {
    return new ArrayList<>(
        ImmutableList.of(
            new RobuxTableScanRule(
                LogicalTableScan.class,
                Convention.NONE,
                RobuxLogicalConvention.instance(),
                RobuxTableScanRule.class.getSimpleName()
            ),
            new RobuxAggregateRule(
                LogicalAggregate.class,
                Convention.NONE,
                RobuxLogicalConvention.instance(),
                RobuxAggregateRule.class.getSimpleName(),
                plannerContext
            ),
            new RobuxSortRule(
                LogicalSort.class,
                Convention.NONE,
                RobuxLogicalConvention.instance(),
                RobuxSortRule.class.getSimpleName()
            ),
            new RobuxProjectRule(
                LogicalProject.class,
                Convention.NONE,
                RobuxLogicalConvention.instance(),
                RobuxProjectRule.class.getSimpleName()
            ),
            new RobuxFilterRule(
                LogicalFilter.class,
                Convention.NONE,
                RobuxLogicalConvention.instance(),
                RobuxFilterRule.class.getSimpleName()
            ),
            new RobuxValuesRule(
                LogicalValues.class,
                Convention.NONE,
                RobuxLogicalConvention.instance(),
                RobuxValuesRule.class.getSimpleName()
            ),
            new RobuxWindowRule(
                Window.class,
                Convention.NONE,
                RobuxLogicalConvention.instance(),
                RobuxWindowRule.class.getSimpleName()
            ),
            new RobuxUnionRule(
                LogicalUnion.class,
                Convention.NONE,
                RobuxLogicalConvention.instance(),
                RobuxUnionRule.class.getSimpleName()
            ),
            new RobuxJoinRule(
                LogicalJoin.class,
                Convention.NONE,
                RobuxLogicalConvention.instance(),
                RobuxJoinRule.class.getSimpleName()
            ),
            RobuxUnnestRule.INSTANCE
        )
    );
  }
}

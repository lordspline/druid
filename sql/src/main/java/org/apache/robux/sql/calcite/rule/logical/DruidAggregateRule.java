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

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.logical.RobuxAggregate;
import org.apache.robux.sql.calcite.rel.logical.RobuxLogicalConvention;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link ConverterRule} to convert {@link Aggregate} to {@link RobuxAggregate}
 */
public class RobuxAggregateRule extends ConverterRule
{
  private final PlannerContext plannerContext;

  public RobuxAggregateRule(
      Class<? extends RelNode> clazz,
      RelTrait in,
      RelTrait out,
      String descriptionPrefix,
      PlannerContext plannerContext
  )
  {
    super(clazz, in, out, descriptionPrefix);
    this.plannerContext = plannerContext;
  }

  @Override
  public RelNode convert(RelNode rel)
  {
    LogicalAggregate aggregate = (LogicalAggregate) rel;
    RelTraitSet newTrait = deriveTraits(aggregate, aggregate.getTraitSet());
    return new RobuxAggregate(
        aggregate.getCluster(),
        newTrait,
        convert(aggregate.getInput(), aggregate.getInput().getTraitSet().replace(RobuxLogicalConvention.instance())),
        aggregate.getGroupSet(),
        aggregate.getGroupSets(),
        aggregate.getAggCallList(),
        plannerContext
    );
  }

  private RelTraitSet deriveTraits(Aggregate aggregate, RelTraitSet traits)
  {
    final RelCollation collation = traits.getTrait(RelCollationTraitDef.INSTANCE);
    if ((collation == null || collation.getFieldCollations().isEmpty()) && aggregate.getGroupSets().size() == 1) {
      // Robux sorts by grouping keys when grouping. Add the collation.
      // Note: [aggregate.getGroupSets().size() == 1] above means that collation isn't added for GROUPING SETS.
      final List<RelFieldCollation> sortFields = new ArrayList<>();
      for (int i = 0; i < aggregate.getGroupCount(); i++) {
        sortFields.add(new RelFieldCollation(i));
      }
      return traits.replace(RobuxLogicalConvention.instance()).replace(RelCollations.of(sortFields));
    }
    return traits.replace(RobuxLogicalConvention.instance());
  }
}

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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.robux.error.InvalidSqlInput;
import org.apache.robux.sql.calcite.rel.logical.RobuxJoin;
import org.apache.robux.sql.calcite.rel.logical.RobuxLogicalConvention;
import org.apache.robux.sql.calcite.rule.RobuxJoinRule.ConditionAnalysis;
import org.checkerframework.checker.nullness.qual.Nullable;

public class RobuxJoinRule extends ConverterRule
{

  public RobuxJoinRule(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String descriptionPrefix)
  {
    super(Config.INSTANCE.withConversion(clazz, in, out, descriptionPrefix));
  }

  @Override
  public @Nullable RelNode convert(RelNode rel)
  {
    Join join = (Join) rel;
    RelTraitSet newTrait = join.getTraitSet().replace(RobuxLogicalConvention.instance());

    ConditionAnalysis analysis = org.apache.robux.sql.calcite.rule.RobuxJoinRule.analyzeCondition(
        join.getCondition(),
        join.getLeft().getRowType(),
        join.getCluster().getRexBuilder()
    );

    if (analysis.errorStr != null) {
      // reject the query in case the anaysis detected any issues
      throw InvalidSqlInput.exception(analysis.errorStr);
    }
    return new RobuxJoin(
        join.getCluster(),
        newTrait,
        join.getHints(),
        convert(
            join.getLeft(),
            RobuxLogicalConvention.instance()
        ),
        convert(
            join.getRight(),
            RobuxLogicalConvention.instance()
        ),
        analysis.getConditionWithUnsupportedSubConditionsIgnored(join.getCluster().getRexBuilder()),
        join.getVariablesSet(),
        join.getJoinType()
    );
  }

  public static boolean isSupportedPredicate(Join join, JoinRelType joinType, RexNode exp)
  {
    ConditionAnalysis analysis = org.apache.robux.sql.calcite.rule.RobuxJoinRule.analyzeCondition(
        exp,
        join.getLeft().getRowType(),
        join.getCluster().getRexBuilder()
    );
    return analysis.errorStr == null;
  }
}

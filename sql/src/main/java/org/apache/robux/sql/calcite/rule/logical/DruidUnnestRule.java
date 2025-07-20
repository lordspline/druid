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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.robux.sql.calcite.rel.logical.RobuxLogicalConvention;

public class RobuxUnnestRule extends ConverterRule
{
  private static Config CONFIG = Config.INSTANCE.withConversion(
      LogicalUnnest.class,
      Convention.NONE,
      RobuxLogicalConvention.instance(),
      RobuxUnnestRule.class.getSimpleName()
  );

  public static final RobuxUnnestRule INSTANCE = new RobuxUnnestRule(CONFIG);

  private RobuxUnnestRule(Config config)
  {
    super(config);
  }

  @Override
  public RelNode convert(RelNode rel)
  {
    LogicalUnnest unnest = (LogicalUnnest) rel;
    RelTraitSet newTrait = unnest.getTraitSet().replace(RobuxLogicalConvention.instance());

    return new RobuxUnnest(
        rel.getCluster(),
        newTrait,
        convert(
            unnest.getInput(),
            RobuxLogicalConvention.instance()
        ),
        unnest.getUnnestExpr(),
        unnest.unnestFieldType,
        unnest.filter
    );
  }
}

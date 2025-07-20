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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.RobuxOuterQueryRel;
import org.apache.robux.sql.calcite.rel.RobuxRel;
import org.apache.robux.sql.calcite.rel.PartialRobuxQuery;
import org.apache.robux.sql.calcite.run.EngineFeature;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class RobuxRules
{
  @SuppressWarnings("rawtypes")
  public static final Predicate<RobuxRel> CAN_BUILD_ON = robuxRel -> robuxRel.getPartialRobuxQuery() != null;

  private RobuxRules()
  {
    // No instantiation.
  }

  public static List<RelOptRule> rules(PlannerContext plannerContext)
  {
    final ArrayList<RelOptRule> retVal = new ArrayList<>(
        ImmutableList.of(
            new RobuxQueryRule<>(
                Filter.class,
                PartialRobuxQuery.Stage.WHERE_FILTER,
                PartialRobuxQuery::withWhereFilter
            ),
            new RobuxQueryRule<>(
                Project.class,
                PartialRobuxQuery.Stage.SELECT_PROJECT,
                PartialRobuxQuery::withSelectProject
            ),
            new RobuxQueryRule<>(
                Aggregate.class,
                PartialRobuxQuery.Stage.AGGREGATE,
                PartialRobuxQuery::withAggregate
            ),
            new RobuxQueryRule<>(
                Project.class,
                PartialRobuxQuery.Stage.AGGREGATE_PROJECT,
                PartialRobuxQuery::withAggregateProject
            ),
            new RobuxQueryRule<>(
                Filter.class,
                PartialRobuxQuery.Stage.HAVING_FILTER,
                PartialRobuxQuery::withHavingFilter
            ),
            new RobuxQueryRule<>(
                Sort.class,
                PartialRobuxQuery.Stage.SORT,
                PartialRobuxQuery::withSort
            ),
            new RobuxQueryRule<>(
                Project.class,
                PartialRobuxQuery.Stage.SORT_PROJECT,
                PartialRobuxQuery::withSortProject
            ),
            RobuxOuterQueryRule.AGGREGATE,
            RobuxOuterQueryRule.WHERE_FILTER,
            RobuxOuterQueryRule.SELECT_PROJECT,
            RobuxOuterQueryRule.SORT,
            new RobuxUnionRule(plannerContext), // Add top level union rule since it helps in constructing a cleaner error message for the user
            new RobuxUnionDataSourceRule(plannerContext),
            RobuxJoinRule.instance(plannerContext)
        )
    );

    if (plannerContext.featureAvailable(EngineFeature.ALLOW_TOP_LEVEL_UNION_ALL)) {
      retVal.add(RobuxSortUnionRule.instance());
    }

    if (plannerContext.featureAvailable(EngineFeature.WINDOW_FUNCTIONS)) {
      retVal.add(new RobuxQueryRule<>(Window.class, PartialRobuxQuery.Stage.WINDOW, PartialRobuxQuery::withWindow));
      retVal.add(
          new RobuxQueryRule<>(
              Project.class,
              PartialRobuxQuery.Stage.WINDOW_PROJECT,
              Project::isMapping, // We can remap fields, but not apply expressions
              PartialRobuxQuery::withWindowProject
          )
      );
      retVal.add(RobuxOuterQueryRule.WINDOW);
    }

    // Adding unnest specific rules
    if (plannerContext.featureAvailable(EngineFeature.UNNEST)) {
      retVal.add(new RobuxUnnestRule(plannerContext));
      retVal.add(new RobuxCorrelateUnnestRule(plannerContext));
      retVal.add(CoreRules.PROJECT_CORRELATE_TRANSPOSE);
      retVal.add(RobuxFilterUnnestRule.instance());
      retVal.add(RobuxFilterUnnestRule.RobuxProjectOnUnnestRule.instance());
    }

    return retVal;
  }

  public static class RobuxQueryRule<RelType extends RelNode> extends RelOptRule
  {
    private final PartialRobuxQuery.Stage stage;
    private final Predicate<RelType> matchesFn;
    private final BiFunction<PartialRobuxQuery, RelType, PartialRobuxQuery> applyFn;

    public RobuxQueryRule(
        final Class<RelType> relClass,
        final PartialRobuxQuery.Stage stage,
        final Predicate<RelType> matchesFn,
        final BiFunction<PartialRobuxQuery, RelType, PartialRobuxQuery> applyFn
    )
    {
      super(
          operand(relClass, operandJ(RobuxRel.class, null, CAN_BUILD_ON, any())),
          StringUtils.format("%s(%s)", RobuxQueryRule.class.getSimpleName(), stage)
      );
      this.stage = stage;
      this.matchesFn = matchesFn;
      this.applyFn = applyFn;
    }

    public RobuxQueryRule(
        final Class<RelType> relClass,
        final PartialRobuxQuery.Stage stage,
        final BiFunction<PartialRobuxQuery, RelType, PartialRobuxQuery> applyFn
    )
    {
      this(relClass, stage, r -> true, applyFn);
    }

    @Override
    public boolean matches(final RelOptRuleCall call)
    {
      final RelType otherRel = call.rel(0);
      final RobuxRel<?> robuxRel = call.rel(1);
      return robuxRel.getPartialRobuxQuery().canAccept(stage) && matchesFn.test(otherRel);
    }

    @Override
    public void onMatch(final RelOptRuleCall call)
    {
      final RelType otherRel = call.rel(0);
      final RobuxRel<?> robuxRel = call.rel(1);

      final PartialRobuxQuery newPartialRobuxQuery = applyFn.apply(robuxRel.getPartialRobuxQuery(), otherRel);
      final RobuxRel<?> newRobuxRel = robuxRel.withPartialQuery(newPartialRobuxQuery);

      if (newRobuxRel.isValidRobuxQuery()) {
        call.transformTo(newRobuxRel);
      }
    }
  }

  public abstract static class RobuxOuterQueryRule extends RelOptRule
  {
    public static final RelOptRule AGGREGATE = new RobuxOuterQueryRule(
        PartialRobuxQuery.Stage.AGGREGATE,
        operand(Aggregate.class, operandJ(RobuxRel.class, null, CAN_BUILD_ON, any())),
        "AGGREGATE"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Aggregate aggregate = call.rel(0);
        final RobuxRel robuxRel = call.rel(1);

        final RobuxOuterQueryRel outerQueryRel = RobuxOuterQueryRel.create(
            robuxRel,
            PartialRobuxQuery.createOuterQuery(robuxRel.getPartialRobuxQuery(), robuxRel.getPlannerContext())
                             .withAggregate(aggregate)
        );
        if (outerQueryRel.isValidRobuxQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule WHERE_FILTER = new RobuxOuterQueryRule(
        PartialRobuxQuery.Stage.WHERE_FILTER,
        operand(Filter.class, operandJ(RobuxRel.class, null, CAN_BUILD_ON, any())),
        "WHERE_FILTER"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Filter filter = call.rel(0);
        final RobuxRel robuxRel = call.rel(1);

        final RobuxOuterQueryRel outerQueryRel = RobuxOuterQueryRel.create(
            robuxRel,
            PartialRobuxQuery.createOuterQuery(robuxRel.getPartialRobuxQuery(), robuxRel.getPlannerContext())
                             .withWhereFilter(filter)
        );
        if (outerQueryRel.isValidRobuxQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule SELECT_PROJECT = new RobuxOuterQueryRule(
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        operand(Project.class, operandJ(RobuxRel.class, null, CAN_BUILD_ON, any())),
        "SELECT_PROJECT"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Project filter = call.rel(0);
        final RobuxRel robuxRel = call.rel(1);

        final RobuxOuterQueryRel outerQueryRel = RobuxOuterQueryRel.create(
            robuxRel,
            PartialRobuxQuery.createOuterQuery(robuxRel.getPartialRobuxQuery(), robuxRel.getPlannerContext())
                             .withSelectProject(filter)
        );
        if (outerQueryRel.isValidRobuxQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule SORT = new RobuxOuterQueryRule(
        PartialRobuxQuery.Stage.SORT,
        operand(Sort.class, operandJ(RobuxRel.class, null, CAN_BUILD_ON, any())),
        "SORT"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Sort sort = call.rel(0);
        final RobuxRel robuxRel = call.rel(1);

        final RobuxOuterQueryRel outerQueryRel = RobuxOuterQueryRel.create(
            robuxRel,
            PartialRobuxQuery.createOuterQuery(robuxRel.getPartialRobuxQuery(), robuxRel.getPlannerContext())
                             .withSort(sort)
        );
        if (outerQueryRel.isValidRobuxQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    public static final RelOptRule WINDOW = new RobuxOuterQueryRule(
        PartialRobuxQuery.Stage.WINDOW,
        operand(Window.class, operandJ(RobuxRel.class, null, CAN_BUILD_ON, any())),
        "WINDOW"
    )
    {
      @Override
      public void onMatch(final RelOptRuleCall call)
      {
        final Window window = call.rel(0);
        final RobuxRel robuxRel = call.rel(1);

        final RobuxOuterQueryRel outerQueryRel = RobuxOuterQueryRel.create(
            robuxRel,
            PartialRobuxQuery.createOuterQuery(robuxRel.getPartialRobuxQuery(), robuxRel.getPlannerContext())
                             .withWindow(window)
        );
        if (outerQueryRel.isValidRobuxQuery()) {
          call.transformTo(outerQueryRel);
        }
      }
    };

    private final PartialRobuxQuery.Stage stage;

    public RobuxOuterQueryRule(
        final PartialRobuxQuery.Stage stage,
        final RelOptRuleOperand op,
        final String description
    )
    {
      super(op, StringUtils.format("%s(%s)", RobuxOuterQueryRel.class.getSimpleName(), description));
      this.stage = stage;
    }

    @Override
    public boolean matches(final RelOptRuleCall call)
    {
      final RobuxRel<?> lowerRobuxRel = call.rel(call.getRelList().size() - 1);
      final RelNode lowerRel = lowerRobuxRel.getPartialRobuxQuery().leafRel();
      final PartialRobuxQuery.Stage lowerStage = lowerRobuxRel.getPartialRobuxQuery().stage();

      if (stage.canFollow(lowerStage)
          || (stage == PartialRobuxQuery.Stage.WHERE_FILTER
              && PartialRobuxQuery.Stage.HAVING_FILTER.canFollow(lowerStage))
          || (stage == PartialRobuxQuery.Stage.SELECT_PROJECT
             && PartialRobuxQuery.Stage.SORT_PROJECT.canFollow(lowerStage))) {
        // Don't consider cases that can be fused into a single query.
        return false;
      } else if (stage == PartialRobuxQuery.Stage.WHERE_FILTER && lowerRel instanceof Filter) {
        // Don't consider filter-on-filter. FilterMergeRule will handle it.
        return false;
      } else if (stage == PartialRobuxQuery.Stage.WHERE_FILTER
                 && lowerStage == PartialRobuxQuery.Stage.SELECT_PROJECT) {
        // Don't consider filter-on-project. ProjectFilterTransposeRule will handle it by swapping them.
        return false;
      } else if (stage == PartialRobuxQuery.Stage.SELECT_PROJECT && lowerRel instanceof Project) {
        // Don't consider project-on-project. ProjectMergeRule will handle it.
        return false;
      } else {
        // Consider subqueries in all other cases.
        return true;
      }
    }
  }
}

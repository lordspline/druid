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
import org.apache.calcite.util.mapping.Mappings;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.rel.RobuxQueryRel;
import org.apache.robux.sql.calcite.rel.RobuxRel;
import org.apache.robux.sql.calcite.rel.RobuxRels;
import org.apache.robux.sql.calcite.rel.RobuxUnionDataSourceRel;
import org.apache.robux.sql.calcite.rel.PartialRobuxQuery;
import org.apache.robux.sql.calcite.table.RobuxTable;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Creates a {@link RobuxUnionDataSourceRel} from various {@link RobuxQueryRel} inputs that represent simple
 * table scans.
 */
public class RobuxUnionDataSourceRule extends RelOptRule
{
  private final PlannerContext plannerContext;

  public RobuxUnionDataSourceRule(final PlannerContext plannerContext)
  {
    super(
        operand(
            Union.class,
            operand(RobuxRel.class, none()),
            operand(RobuxQueryRel.class, none())
        )
    );
    this.plannerContext = plannerContext;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final RobuxRel<?> firstRobuxRel = call.rel(1);
    final RobuxQueryRel secondRobuxRel = call.rel(2);

    return isCompatible(unionRel, firstRobuxRel, secondRobuxRel, plannerContext);
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Union unionRel = call.rel(0);
    final RobuxRel<?> firstRobuxRel = call.rel(1);
    final RobuxQueryRel secondRobuxRel = call.rel(2);

    if (firstRobuxRel instanceof RobuxUnionDataSourceRel) {
      // Unwrap and flatten the inputs to the Union.
      final RelNode newUnionRel = call.builder()
                                      .pushAll(firstRobuxRel.getInputs())
                                      .push(secondRobuxRel)
                                      .union(true, firstRobuxRel.getInputs().size() + 1)
                                      .build();

      call.transformTo(
          RobuxUnionDataSourceRel.create(
              (Union) newUnionRel,
              getColumnNamesIfTableOrUnion(firstRobuxRel, plannerContext).get(),
              firstRobuxRel.getPlannerContext()
          )
      );
    } else {
      // Sanity check.
      if (!(firstRobuxRel instanceof RobuxQueryRel)) {
        throw new ISE("Expected first rel to be a RobuxQueryRel, but it was %s", firstRobuxRel.getClass().getName());
      }

      call.transformTo(
          RobuxUnionDataSourceRel.create(
              unionRel,
              getColumnNamesIfTableOrUnion(firstRobuxRel, plannerContext).get(),
              firstRobuxRel.getPlannerContext()
          )
      );
    }
  }

  // Can only do UNION ALL of inputs that have compatible schemas (or schema mappings) and right side
  // is a simple table scan
  public static boolean isCompatible(
      final Union unionRel,
      final RobuxRel<?> first,
      final RobuxRel<?> second,
      @Nullable PlannerContext plannerContext
  )
  {
    if (!(second instanceof RobuxQueryRel)) {
      return false;
    }

    if (!unionRel.all && null != plannerContext) {
      plannerContext.setPlanningError("SQL requires 'UNION' but only 'UNION ALL' is supported.");
    }
    return unionRel.all && isUnionCompatible(first, second, plannerContext);
  }

  private static boolean isUnionCompatible(final RobuxRel<?> first, final RobuxRel<?> second, @Nullable PlannerContext plannerContext)
  {
    final Optional<List<String>> firstColumnNames = getColumnNamesIfTableOrUnion(first, plannerContext);
    final Optional<List<String>> secondColumnNames = getColumnNamesIfTableOrUnion(second, plannerContext);
    if (!firstColumnNames.isPresent() || !secondColumnNames.isPresent()) {
      // No need to set the planning error here
      return false;
    }
    if (!firstColumnNames.equals(secondColumnNames)) {
      if (null != plannerContext) {
        plannerContext.setPlanningError("SQL requires union between two tables and column names queried for " +
            "each table are different Left: %s, Right: %s.",
            firstColumnNames.orElse(Collections.emptyList()),
            secondColumnNames.orElse(Collections.emptyList()));
      }
      return false;
    }
    return true;
  }

  static Optional<List<String>> getColumnNamesIfTableOrUnion(final RobuxRel<?> robuxRel, @Nullable PlannerContext plannerContext)
  {
    final PartialRobuxQuery partialQuery = robuxRel.getPartialRobuxQuery();

    final Optional<RobuxTable> robuxTable =
        RobuxRels.robuxTableIfLeafRel(robuxRel)
                 .filter(table -> table.getDataSource() instanceof TableDataSource);

    if (robuxTable.isPresent() && RobuxRels.isScanOrMapping(robuxRel, false)) {
      // This rel is a table scan or mapping.

      if (partialQuery.stage() == PartialRobuxQuery.Stage.SCAN) {
        return Optional.of(robuxTable.get().getRowSignature().getColumnNames());
      } else {
        // Sanity check. Expected to be true due to the "scan or mapping" check.
        if (partialQuery.stage() != PartialRobuxQuery.Stage.SELECT_PROJECT) {
          throw new ISE("Expected stage %s but got %s", PartialRobuxQuery.Stage.SELECT_PROJECT, partialQuery.stage());
        }

        // Apply the mapping (with additional sanity checks).
        final RowSignature tableSignature = robuxTable.get().getRowSignature();
        final Mappings.TargetMapping mapping = partialQuery.getSelectProject().getMapping();

        if (mapping.getSourceCount() != tableSignature.size()) {
          throw new ISE(
              "Expected mapping with %d columns but got %d columns",
              tableSignature.size(),
              mapping.getSourceCount()
          );
        }

        final List<String> retVal = new ArrayList<>();

        for (int i = 0; i < mapping.getTargetCount(); i++) {
          final int sourceField = mapping.getSourceOpt(i);
          retVal.add(tableSignature.getColumnName(sourceField));
        }

        return Optional.of(retVal);
      }
    } else if (!robuxTable.isPresent() && robuxRel instanceof RobuxUnionDataSourceRel) {
      // This rel is a union itself.

      return Optional.of(((RobuxUnionDataSourceRel) robuxRel).getUnionColumnNames());
    } else if (robuxTable.isPresent()) {
      if (null != plannerContext) {
        plannerContext.setPlanningError("SQL requires union between inputs that are not simple table scans " +
            "and involve a filter or aliasing. Or column types of tables being unioned are not of same type.");
      }
      return Optional.empty();
    } else {
      if (null != plannerContext) {
        plannerContext.setPlanningError("SQL requires union with input of a datasource type that is not supported."
            + " Union operation is only supported between regular tables. ");
      }
      return Optional.empty();
    }
  }
}

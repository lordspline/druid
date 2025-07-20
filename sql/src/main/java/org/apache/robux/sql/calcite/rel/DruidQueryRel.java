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

package org.apache.robux.sql.calcite.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.robux.query.InlineDataSource;
import org.apache.robux.sql.calcite.external.ExternalTableScan;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.table.RobuxTable;
import org.apache.robux.sql.calcite.table.InlineTable;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * RobuxRel that operates on top of a {@link RobuxTable} directly (no joining or subqueries).
 */
public class RobuxQueryRel extends RobuxRel<RobuxQueryRel>
{
  @Nullable
  private final RelOptTable table; // must not be null except for inline data
  private final RobuxTable robuxTable;
  private final PartialRobuxQuery partialQuery;

  private RobuxQueryRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      @Nullable final RelOptTable table,
      final RobuxTable robuxTable,
      final PlannerContext plannerContext,
      final PartialRobuxQuery partialQuery
  )
  {
    super(cluster, traitSet, plannerContext);
    this.table = table;
    this.robuxTable = Preconditions.checkNotNull(robuxTable, "robuxTable");
    this.partialQuery = Preconditions.checkNotNull(partialQuery, "partialQuery");
  }

  /**
   * Create a RobuxQueryRel representing a full scan of a builtin table or lookup.
   */
  public static RobuxQueryRel scanTable(
      final LogicalTableScan scanRel,
      final RelOptTable table,
      final RobuxTable robuxTable,
      final PlannerContext plannerContext
  )
  {
    return new RobuxQueryRel(
        scanRel.getCluster(),
        scanRel.getCluster().traitSetOf(Convention.NONE),
        Preconditions.checkNotNull(table, "table"),
        robuxTable,
        plannerContext,
        PartialRobuxQuery.create(scanRel)
    );
  }

  /**
   * Create a RobuxQueryRel representing a full scan of external data.
   */
  public static RobuxQueryRel scanExternal(
      final ExternalTableScan scanRel,
      final PlannerContext plannerContext
  )
  {
    return new RobuxQueryRel(
        scanRel.getCluster(),
        scanRel.getCluster().traitSetOf(Convention.NONE),
        null,
        scanRel.getRobuxTable(),
        plannerContext,
        PartialRobuxQuery.create(scanRel)
    );
  }

  /**
   * Create a RobuxQueryRel representing a full scan of inline, literal values.
   */
  public static RobuxQueryRel scanConstantRel(
      final RelNode rel,
      final InlineDataSource dataSource,
      final PlannerContext plannerContext
  )
  {
    return new RobuxQueryRel(
        rel.getCluster(),
        rel.getTraitSet().replace(Convention.NONE), // keep traitSet of input rel, except for convention
        null,
        new InlineTable(dataSource),
        plannerContext,
        PartialRobuxQuery.create(rel)
    );
  }

  @Override
  public RobuxQuery toRobuxQuery(final boolean finalizeAggregations)
  {
    return partialQuery.build(
        robuxTable.getDataSource(),
        robuxTable.getRowSignature(),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        finalizeAggregations,
        true
    );
  }

  @Override
  public RobuxQuery toRobuxQueryForExplaining()
  {
    return partialQuery.build(
        robuxTable.getDataSource(),
        robuxTable.getRowSignature(),
        getPlannerContext(),
        getCluster().getRexBuilder(),
        false,
        false
    );
  }

  @Override
  public RobuxQueryRel asRobuxConvention()
  {
    return new RobuxQueryRel(
        getCluster(),
        getTraitSet().replace(RobuxConvention.instance()),
        table,
        robuxTable,
        getPlannerContext(),
        partialQuery
    );
  }

  @Override
  public Set<String> getDataSourceNames()
  {
    return robuxTable.getDataSource().getTableNames();
  }

  @Override
  public PartialRobuxQuery getPartialRobuxQuery()
  {
    return partialQuery;
  }

  @Override
  public RobuxQueryRel withPartialQuery(final PartialRobuxQuery newQueryBuilder)
  {
    return new RobuxQueryRel(
        getCluster(),
        newQueryBuilder.getTraitSet(getConvention(), getPlannerContext()),
        table,
        robuxTable,
        getPlannerContext(),
        newQueryBuilder
    );
  }

  public RobuxTable getRobuxTable()
  {
    return robuxTable;
  }

  @Override
  public RelOptTable getTable()
  {
    return table;
  }

  @Override
  protected RelDataType deriveRowType()
  {
    return partialQuery.getRowType();
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw)
  {
    final String queryString;
    final RobuxQuery robuxQuery = toRobuxQueryForExplaining();

    try {
      queryString = getPlannerContext().getJsonMapper().writeValueAsString(robuxQuery.getQuery());
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    return pw.item("query", queryString)
             .item("signature", robuxQuery.getOutputRowSignature());
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq)
  {
    return planner.getCostFactory().makeCost(partialQuery.estimateCost(), 0, 0);
  }
}

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

package org.apache.robux.msq.logical.stages;

import org.apache.robux.msq.exec.StageProcessor;
import org.apache.robux.msq.logical.LogicalInputSpec;
import org.apache.robux.msq.logical.StageMaker;
import org.apache.robux.query.filter.DimFilter;
import org.apache.robux.segment.VirtualColumns;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.planner.querygen.RobuxQueryGenerator.RobuxNodeStack;
import org.apache.robux.sql.calcite.rel.RobuxQuery;
import org.apache.robux.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.robux.sql.calcite.rel.logical.RobuxAggregate;
import org.apache.robux.sql.calcite.rel.logical.RobuxFilter;
import org.apache.robux.sql.calcite.rel.logical.RobuxLogicalNode;
import org.apache.robux.sql.calcite.rel.logical.RobuxProject;

/**
 * Represents a stage that reads data from input sources.
 */
public class ReadStage extends AbstractFrameProcessorStage
{
  public ReadStage(RowSignature signature, LogicalInputSpec inputSpec)
  {
    super(signature, inputSpec);
  }

  /**
   * Copy constructor.
   */
  protected ReadStage(ReadStage readStage, RowSignature newSignature)
  {
    super(newSignature, readStage.inputSpecs);
  }

  @Override
  public LogicalStage extendWith(RobuxNodeStack stack)
  {
    RobuxLogicalNode node = stack.getNode();
    if (node instanceof RobuxFilter) {
      RobuxFilter filter = (RobuxFilter) node;
      return makeFilterStage(stack.getPlannerContext(), filter);
    }

    if (node instanceof RobuxProject || node instanceof RobuxAggregate) {

      RobuxLogicalNode project = node;
      RobuxFilter dummyFilter = new RobuxFilter(
          project.getCluster(), project.getTraitSet(), project,
          project.getCluster().getRexBuilder().makeLiteral(true)
      );
      return makeFilterStage(stack.getPlannerContext(), dummyFilter).extendWith(stack);
    }
    return null;
  }

  private LogicalStage makeFilterStage(PlannerContext plannerContext, RobuxFilter filter)
  {
    VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        signature,
        plannerContext.getExpressionParser(),
        plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
    );

    DimFilter dimFilter = RobuxQuery.getDimFilter(
        plannerContext,
        signature, virtualColumnRegistry, filter
    );

    return new FilterStage(
        this,
        virtualColumnRegistry,
        dimFilter
    );
  }

  @Override
  public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
  {
    return StageMaker.makeScanStageProcessor(VirtualColumns.EMPTY, signature, null);
  }
}

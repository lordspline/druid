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

package org.apache.robux.msq.logical;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.robux.error.RobuxException;
import org.apache.robux.frame.key.KeyColumn;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.msq.input.inline.InlineInputSpec;
import org.apache.robux.msq.input.table.TableInputSpec;
import org.apache.robux.msq.logical.stages.LogicalStage;
import org.apache.robux.msq.logical.stages.OffsetLimitStage;
import org.apache.robux.msq.logical.stages.ReadStage;
import org.apache.robux.msq.logical.stages.SortStage;
import org.apache.robux.query.InlineDataSource;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.query.groupby.orderby.OrderByColumnSpec;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.planner.querygen.RobuxQueryGenerator.RobuxNodeStack;
import org.apache.robux.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.robux.sql.calcite.rel.RobuxQuery;
import org.apache.robux.sql.calcite.rel.logical.RobuxLogicalNode;
import org.apache.robux.sql.calcite.rel.logical.RobuxSort;
import org.apache.robux.sql.calcite.rel.logical.RobuxTableScan;
import org.apache.robux.sql.calcite.rel.logical.RobuxValues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Translates the logical plan defined by the {@link RobuxLogicalNode} into a
 * {@link LogicalStage} nodes.
 *
 * The translation should be executed as a single pass over the logical plan.
 *
 * During translation all stages have access to {@link RobuxNodeStack} which
 * contain all current parents of the current stage.
 */
public class RobuxLogicalToQueryDefinitionTranslator
{
  private PlannerContext plannerContext;

  public RobuxLogicalToQueryDefinitionTranslator(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
  }

  /**
   * Executes the translation of the logical plan into a query definition.
   */
  public LogicalStage translate(RobuxLogicalNode relRoot)
  {
    RobuxNodeStack stack = new RobuxNodeStack(plannerContext);
    stack.push(relRoot);
    LogicalStage logicalStage = buildStageFor(stack);
    return logicalStage;
  }

  /**
   * Builds a logical stage for the given input.
   *
   * Conducts building the stage by first building all the inputs and then the stage itself.
   * It should build the stage corresponding to stack#peekNode
   *
   * @throws RobuxException if the stage cannot be built
   */
  private LogicalStage buildStageFor(RobuxNodeStack stack)
  {
    List<LogicalStage> inputStages = buildInputStages(stack);
    RobuxLogicalNode node = stack.getNode();
    if (inputStages.size() == 0) {
      Optional<ReadStage> stage = buildReadStage(node);
      if (stage.isPresent()) {
        return stage.get();
      }
    }
    if (inputStages.size() == 1) {
      LogicalStage inputStage = inputStages.get(0);
      LogicalStage newStage = inputStage.extendWith(stack);
      if (newStage != null) {
        return newStage;
      }
      newStage = makeSequenceStage(inputStage, stack);
      if (newStage != null) {
        return newStage;
      }
    }
    throw RobuxException.defensive().build("Unable to process relNode[%s]", node);
  }

  private Optional<ReadStage> buildReadStage(RobuxLogicalNode node)
  {
    if (node instanceof RobuxValues) {
      return translateValues((RobuxValues) node);
    }
    if (node instanceof RobuxTableScan) {
      return translateTableScan((RobuxTableScan) node);
    }
    return Optional.empty();
  }

  private LogicalStage makeSequenceStage(LogicalStage inputStage, RobuxNodeStack stack)
  {
    if (stack.getNode() instanceof RobuxSort) {
      RobuxSort sort = (RobuxSort) stack.getNode();
      List<OrderByColumnSpec> orderBySpecs = RobuxQuery.buildOrderByColumnSpecs(inputStage.getLogicalRowSignature(), sort);
      List<KeyColumn> keyColumns = Lists.transform(orderBySpecs, KeyColumn::fromOrderByColumnSpec);
      SortStage sortStage = new SortStage(inputStage, keyColumns);

      if (sort.hasLimitOrOffset()) {
        return new OffsetLimitStage(sortStage, sort.getOffsetLimit());
      } else {
        return sortStage;
      }
    }
    return new ReadStage(inputStage.getLogicalRowSignature(), LogicalInputSpec.of(inputStage)).extendWith(stack);
  }


  private List<LogicalStage> buildInputStages(RobuxNodeStack stack)
  {
    List<LogicalStage> inputStages = new ArrayList<>();
    List<RelNode> inputs = stack.getNode().getInputs();
    for (RelNode input : inputs) {
      stack.push((RobuxLogicalNode) input, inputStages.size());
      inputStages.add(buildStageFor(stack));
      stack.pop();
    }
    return inputStages;
  }

  private Optional<ReadStage> translateTableScan(RobuxTableScan node)
  {
    SourceDesc sd = node.getSourceDesc(plannerContext, Collections.emptyList());
    TableDataSource ids = (TableDataSource) sd.dataSource;
    TableInputSpec inputSpec = new TableInputSpec(ids.getName(), Intervals.ONLY_ETERNITY, null, null);
    ReadStage stage = new ReadStage(sd.rowSignature, LogicalInputSpec.of(inputSpec));
    return Optional.of(stage);
  }

  private Optional<ReadStage> translateValues(RobuxValues node)
  {
    SourceDesc sd = node.getSourceDesc(plannerContext, Collections.emptyList());
    InlineDataSource ids = (InlineDataSource) sd.dataSource;
    InlineInputSpec inputSpec = new InlineInputSpec(ids);
    ReadStage stage = new ReadStage(sd.rowSignature, LogicalInputSpec.of(inputSpec));
    return Optional.of(stage);
  }
}

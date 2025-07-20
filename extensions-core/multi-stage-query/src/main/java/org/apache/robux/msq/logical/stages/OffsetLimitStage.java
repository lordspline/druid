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

import com.google.common.collect.ImmutableList;
import org.apache.robux.msq.exec.StageProcessor;
import org.apache.robux.msq.logical.LogicalInputSpec;
import org.apache.robux.msq.logical.StageMaker;
import org.apache.robux.msq.querykit.common.OffsetLimitStageProcessor;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.planner.OffsetLimit;
import org.apache.robux.sql.calcite.planner.querygen.RobuxQueryGenerator.RobuxNodeStack;

public class OffsetLimitStage extends AbstractFrameProcessorStage
{
  protected final OffsetLimit offsetLimit;

  public OffsetLimitStage(LogicalStage inputStage, OffsetLimit offsetLimit)
  {
    super(inputStage.getRowSignature(), ImmutableList.of(LogicalInputSpec.of(inputStage)));
    this.offsetLimit = offsetLimit;
  }

  @Override
  public LogicalStage extendWith(RobuxNodeStack stack)
  {
    return null;
  }

  @Override
  public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
  {
    return new OffsetLimitStageProcessor(
        offsetLimit.getOffset(),
        offsetLimit.hasLimit() ? offsetLimit.getLimit() : null
    );
  }

  @Override
  public RowSignature getLogicalRowSignature()
  {
    return inputSpecs.get(0).getRowSignature();
  }
}

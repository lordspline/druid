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

import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.table.RobuxTable;

import java.util.Optional;

public class RobuxRels
{
  /**
   * Returns the DataSource involved in a leaf query of class {@link RobuxQueryRel}.
   */
  public static Optional<RobuxTable> robuxTableIfLeafRel(final RobuxRel<?> robuxRel)
  {
    if (robuxRel instanceof RobuxQueryRel) {
      return Optional.of(((RobuxQueryRel) robuxRel).getRobuxTable());
    } else {
      return Optional.empty();
    }
  }

  /**
   * Check if a robuxRel is a simple table scan, or a projection that merely remaps columns without transforming them.
   * Like {@link #isScanOrProject} but more restrictive: only remappings are allowed.
   *
   * @param robuxRel         the rel to check
   * @param canBeJoinOrUnion consider a {@link RobuxJoinQueryRel} or {@link RobuxUnionDataSourceRel} as possible
   *                         scans-and-mappings too.
   */
  public static boolean isScanOrMapping(final RobuxRel<?> robuxRel, final boolean canBeJoinOrUnion)
  {
    if (isScanOrProject(robuxRel, canBeJoinOrUnion)) {
      // Like isScanOrProject, but don't allow transforming projections.
      final PartialRobuxQuery partialQuery = robuxRel.getPartialRobuxQuery();
      return partialQuery.getSelectProject() == null || partialQuery.getSelectProject().isMapping();
    } else {
      return false;
    }
  }

  /**
   * Check if a robuxRel is a simple table scan or a scan + projection.
   *
   * @param robuxRel         the rel to check
   * @param canBeJoinOrUnion consider a {@link RobuxJoinQueryRel} or {@link RobuxUnionDataSourceRel} as possible
   *                         scans-and-mappings too.
   */
  public static boolean isScanOrProject(final RobuxRel<?> robuxRel, final boolean canBeJoinOrUnion)
  {
    if (robuxRel instanceof RobuxQueryRel || (canBeJoinOrUnion && (robuxRel instanceof RobuxJoinQueryRel || robuxRel instanceof RobuxCorrelateUnnestRel
                                                                   || robuxRel instanceof RobuxUnionDataSourceRel))) {
      final PartialRobuxQuery partialQuery = robuxRel.getPartialRobuxQuery();
      final PartialRobuxQuery.Stage stage = partialQuery.stage();
      return (stage == PartialRobuxQuery.Stage.SCAN || stage == PartialRobuxQuery.Stage.SELECT_PROJECT)
             && partialQuery.getWhereFilter() == null;
    } else {
      return false;
    }
  }

  /**
   * Returns the signature of the datasource of a {@link RobuxRel}.
   *
   * This is not the signature of the {@link RobuxRel} itself: in particular, it ignores any operations that are layered
   * on top of the datasource.
   */
  public static RowSignature dataSourceSignature(final RobuxRel<?> robuxRel)
  {
    if (robuxRel instanceof RobuxQueryRel) {
      // Get signature directly from the table.
      return ((RobuxQueryRel) robuxRel).getRobuxTable().getRowSignature();
    } else {
      // Build the query with a no-op PartialRobuxQuery.
      return robuxRel.withPartialQuery(
          PartialRobuxQuery.create(robuxRel.getPartialRobuxQuery().getScan())
      ).toRobuxQuery(false).getOutputRowSignature();
    }
  }
}

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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.sql.calcite.rel.RobuxJoinQueryRel;
import org.apache.robux.sql.calcite.rel.RobuxOuterQueryRel;
import org.apache.robux.sql.calcite.rel.RobuxQueryRel;
import org.apache.robux.sql.calcite.rel.RobuxRel;
import org.apache.robux.sql.calcite.rel.RobuxRelsTest;
import org.apache.robux.sql.calcite.rel.RobuxUnionDataSourceRel;
import org.apache.robux.sql.calcite.rel.PartialRobuxQuery;
import org.apache.robux.sql.calcite.table.DatasourceTable;
import org.apache.robux.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.robux.sql.calcite.table.RobuxTable;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class RobuxUnionDataSourceRuleTest
{
  private final RobuxTable fooRobuxTable = new DatasourceTable(
      new PhysicalDatasourceMetadata(
          new TableDataSource("foo"),
          RowSignature.builder()
                      .addTimeColumn()
                      .add("col1", ColumnType.STRING)
                      .add("col2", ColumnType.LONG)
                      .build(),
          false,
          false
      )
  );

  @Test
  public void test_getColumnNamesIfTableOrUnion_tableScan()
  {
    final RobuxRel<?> robuxRel = RobuxRelsTest.mockRobuxRel(
        RobuxQueryRel.class,
        PartialRobuxQuery.Stage.SCAN,
        fooRobuxTable,
        null,
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("__time", "col1", "col2")),
        RobuxUnionDataSourceRule.getColumnNamesIfTableOrUnion(robuxRel, null)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_tableMapping()
  {
    final RobuxRel<?> robuxRel = RobuxRelsTest.mockRobuxRel(
        RobuxQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        fooRobuxTable,
        RobuxRelsTest.mockMappingProject(ImmutableList.of(1), 3),
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("col1")),
        RobuxUnionDataSourceRule.getColumnNamesIfTableOrUnion(robuxRel, null)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_tableProject()
  {
    final RobuxRel<?> robuxRel = RobuxRelsTest.mockRobuxRel(
        RobuxQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        fooRobuxTable,
        RobuxRelsTest.mockNonMappingProject(),
        null
    );

    Assert.assertEquals(
        Optional.empty(),
        RobuxUnionDataSourceRule.getColumnNamesIfTableOrUnion(robuxRel, null)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_tableFilterPlusMapping()
  {
    final RobuxRel<?> robuxRel = RobuxRelsTest.mockRobuxRel(
        RobuxQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        fooRobuxTable,
        RobuxRelsTest.mockMappingProject(ImmutableList.of(1), 3),
        RobuxRelsTest.mockFilter()
    );

    Assert.assertEquals(
        Optional.empty(),
        RobuxUnionDataSourceRule.getColumnNamesIfTableOrUnion(robuxRel, null)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_unionScan()
  {
    final RobuxUnionDataSourceRel robuxRel = RobuxRelsTest.mockRobuxRel(
        RobuxUnionDataSourceRel.class,
        rel -> EasyMock.expect(rel.getUnionColumnNames()).andReturn(fooRobuxTable.getRowSignature().getColumnNames()),
        PartialRobuxQuery.Stage.SCAN,
        null,
        null,
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("__time", "col1", "col2")),
        RobuxUnionDataSourceRule.getColumnNamesIfTableOrUnion(robuxRel, null)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_unionMapping()
  {
    final Project project = RobuxRelsTest.mockMappingProject(ImmutableList.of(2, 1), 3);
    final Mappings.TargetMapping mapping = project.getMapping();
    final String[] mappedColumnNames = new String[mapping.getTargetCount()];

    final List<String> columnNames = fooRobuxTable.getRowSignature().getColumnNames();
    for (int i = 0; i < columnNames.size(); i++) {
      mappedColumnNames[mapping.getTargetOpt(i)] = columnNames.get(i);
    }

    final RobuxUnionDataSourceRel robuxRel = RobuxRelsTest.mockRobuxRel(
        RobuxUnionDataSourceRel.class,
        rel -> EasyMock.expect(rel.getUnionColumnNames()).andReturn(Arrays.asList(mappedColumnNames)),
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("col2", "col1")),
        RobuxUnionDataSourceRule.getColumnNamesIfTableOrUnion(robuxRel, null)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_unionProject()
  {
    final RobuxUnionDataSourceRel robuxRel = RobuxRelsTest.mockRobuxRel(
        RobuxUnionDataSourceRel.class,
        rel -> EasyMock.expect(rel.getUnionColumnNames()).andReturn(fooRobuxTable.getRowSignature().getColumnNames()),
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        RobuxRelsTest.mockNonMappingProject(),
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("__time", "col1", "col2")),
        RobuxUnionDataSourceRule.getColumnNamesIfTableOrUnion(robuxRel, null)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_outerQuery()
  {
    final RobuxRel<?> robuxRel = RobuxRelsTest.mockRobuxRel(
        RobuxOuterQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        null,
        null
    );

    Assert.assertEquals(
        Optional.empty(),
        RobuxUnionDataSourceRule.getColumnNamesIfTableOrUnion(robuxRel, null)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_join()
  {
    final RobuxRel<?> robuxRel = RobuxRelsTest.mockRobuxRel(
        RobuxJoinQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        null,
        null
    );

    Assert.assertEquals(
        Optional.empty(),
        RobuxUnionDataSourceRule.getColumnNamesIfTableOrUnion(robuxRel, null)
    );
  }
}

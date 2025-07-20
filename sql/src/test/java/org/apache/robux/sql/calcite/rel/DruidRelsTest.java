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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.robux.sql.calcite.table.RobuxTable;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;

public class RobuxRelsTest
{
  @Test
  public void test_isScanOrMapping_scan()
  {
    final RobuxRel<?> rel = mockRobuxRel(RobuxQueryRel.class, PartialRobuxQuery.Stage.SCAN, null, null, null);
    Assert.assertTrue(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertTrue(RobuxRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialRobuxQuery());
  }

  @Test
  public void test_isScanOrMapping_scanJoin()
  {
    final RobuxRel<?> rel = mockRobuxRel(RobuxJoinQueryRel.class, PartialRobuxQuery.Stage.SCAN, null, null, null);
    Assert.assertTrue(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialRobuxQuery());
  }

  @Test
  public void test_isScanOrMapping_scanUnion()
  {
    final RobuxRel<?> rel = mockRobuxRel(RobuxUnionDataSourceRel.class, PartialRobuxQuery.Stage.SCAN, null, null, null);
    Assert.assertTrue(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialRobuxQuery());
  }

  @Test
  public void test_isScanOrMapping_scanQuery()
  {
    final RobuxRel<?> rel = mockRobuxRel(RobuxOuterQueryRel.class, PartialRobuxQuery.Stage.SCAN, null, null, null);
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialRobuxQuery());
  }

  @Test
  public void test_isScanOrMapping_mapping()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assert.assertTrue(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertTrue(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_mappingJoin()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxJoinQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assert.assertTrue(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_mappingUnion()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxUnionDataSourceRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assert.assertTrue(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_mappingQuery()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxOuterQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_nonMapping()
  {
    final Project project = mockNonMappingProject();
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_nonMappingJoin()
  {
    final Project project = mockNonMappingProject();
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxJoinQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_nonMappingUnion()
  {
    final Project project = mockNonMappingProject();
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxUnionDataSourceRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filterThenProject()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        mockFilter()
    );
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filterThenProjectJoin()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxJoinQueryRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        mockFilter()
    );
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filterThenProjectUnion()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxUnionDataSourceRel.class,
        PartialRobuxQuery.Stage.SELECT_PROJECT,
        null,
        project,
        mockFilter()
    );
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filter()
  {
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxQueryRel.class,
        PartialRobuxQuery.Stage.WHERE_FILTER,
        null,
        null,
        mockFilter()
    );
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery());
  }

  @Test
  public void test_isScanOrMapping_filterJoin()
  {
    final RobuxRel<?> rel = mockRobuxRel(
        RobuxJoinQueryRel.class,
        PartialRobuxQuery.Stage.WHERE_FILTER,
        null,
        null,
        mockFilter()
    );
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, true));
    Assert.assertFalse(RobuxRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialRobuxQuery());
  }

  @Test
  public void test_isScanOrMapping_allStages()
  {
    final ImmutableSet<PartialRobuxQuery.Stage> okStages = ImmutableSet.of(
        PartialRobuxQuery.Stage.SCAN,
        PartialRobuxQuery.Stage.SELECT_PROJECT
    );

    for (PartialRobuxQuery.Stage stage : PartialRobuxQuery.Stage.values()) {
      final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
      final RobuxRel<?> rel = mockRobuxRel(
          RobuxQueryRel.class,
          stage,
          null,
          project,
          null
      );

      Assert.assertEquals(stage.toString(), okStages.contains(stage), RobuxRels.isScanOrMapping(rel, true));
      Assert.assertEquals(stage.toString(), okStages.contains(stage), RobuxRels.isScanOrMapping(rel, false));

      EasyMock.verify(rel, rel.getPartialRobuxQuery(), project);
    }
  }

  public static RobuxRel<?> mockRobuxRel(
      final Class<? extends RobuxRel<?>> clazz,
      final PartialRobuxQuery.Stage stage,
      @Nullable RobuxTable robuxTable,
      @Nullable Project selectProject,
      @Nullable Filter whereFilter
  )
  {
    return mockRobuxRel(clazz, rel -> {}, stage, robuxTable, selectProject, whereFilter);
  }

  public static <T extends RobuxRel<?>> T mockRobuxRel(
      final Class<T> clazz,
      final Consumer<T> additionalExpectationsFunction,
      final PartialRobuxQuery.Stage stage,
      @Nullable RobuxTable robuxTable,
      @Nullable Project selectProject,
      @Nullable Filter whereFilter
  )
  {
    // RobuxQueryRels rely on a ton of Calcite stuff like RelOptCluster, RelOptTable, etc, which is quite verbose to
    // create real instances of. So, tragically, we'll use EasyMock.
    final PartialRobuxQuery mockPartialQuery = EasyMock.mock(PartialRobuxQuery.class);
    EasyMock.expect(mockPartialQuery.stage()).andReturn(stage).anyTimes();
    EasyMock.expect(mockPartialQuery.getSelectProject()).andReturn(selectProject).anyTimes();
    EasyMock.expect(mockPartialQuery.getWhereFilter()).andReturn(whereFilter).anyTimes();

    final RelOptTable mockRelOptTable = EasyMock.mock(RelOptTable.class);

    final T mockRel = EasyMock.mock(clazz);
    EasyMock.expect(mockRel.getPartialRobuxQuery()).andReturn(mockPartialQuery).anyTimes();
    EasyMock.expect(mockRel.getTable()).andReturn(mockRelOptTable).anyTimes();
    if (clazz == RobuxQueryRel.class) {
      EasyMock.expect(((RobuxQueryRel) mockRel).getRobuxTable()).andReturn(robuxTable).anyTimes();
    }
    additionalExpectationsFunction.accept(mockRel);

    EasyMock.replay(mockRel, mockPartialQuery, mockRelOptTable);
    return mockRel;
  }

  public static Project mockMappingProject(final List<Integer> sources, final int sourceCount)
  {
    final Project mockProject = EasyMock.mock(Project.class);
    EasyMock.expect(mockProject.isMapping()).andReturn(true).anyTimes();

    final Mappings.PartialMapping mapping = new Mappings.PartialMapping(sources, sourceCount, MappingType.SURJECTION);

    EasyMock.expect(mockProject.getMapping()).andReturn(mapping).anyTimes();
    EasyMock.replay(mockProject);
    return mockProject;
  }

  public static Project mockNonMappingProject()
  {
    final Project mockProject = EasyMock.mock(Project.class);
    EasyMock.expect(mockProject.isMapping()).andReturn(false).anyTimes();
    EasyMock.replay(mockProject);
    return mockProject;
  }

  public static Filter mockFilter()
  {
    return EasyMock.mock(Filter.class);
  }
}

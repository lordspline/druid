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

package org.apache.robux.sql.calcite.util;

import com.google.common.collect.ImmutableList;
import org.apache.robux.java.util.common.FileUtils;
import org.apache.robux.math.expr.ExpressionProcessing;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.server.security.Action;
import org.apache.robux.server.security.Resource;
import org.apache.robux.server.security.ResourceAction;
import org.apache.robux.server.security.ResourceType;
import org.apache.robux.sql.calcite.expression.RobuxExpression;
import org.apache.robux.sql.calcite.expression.SimpleExtraction;
import org.apache.robux.sql.http.SqlParameter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

public abstract class CalciteTestBase
{
  public static final List<SqlParameter> DEFAULT_PARAMETERS = ImmutableList.of();

  @BeforeAll
  public static void setupCalciteProperties()
  {
    ExpressionProcessing.initializeForTests();
  }

  /**
   * Temporary folder(s).
   *
   * Due to some possible reuse of configuration in the next case; there is only one real temporary path;
   * but every case gets a separate folder in it.
   *
   * note: {@link #rootTempPath} and {@link #casetempPath} are made private to ensure that
   */
  @TempDir
  private static Path rootTempPath;
  private Path casetempPath;

  @BeforeEach
  public void setCaseTempDir(TestInfo testInfo)
  {
    String methodName = testInfo.getTestMethod().get().getName();
    casetempPath = FileUtils.createTempDirInLocation(rootTempPath, methodName).toPath();
  }

  public File newTempFolder()
  {
    return newTempFolder(null);
  }

  public File newTempFolder(String prefix)
  {
    return FileUtils.createTempDirInLocation(casetempPath, prefix);
  }

  public File newTempFile(String prefix)
  {
    try {
      return Files.createTempFile(casetempPath, prefix, null).toFile();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @deprecated prefer to make {@link RobuxExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static RobuxExpression makeColumnExpression(final String column)
  {
    return RobuxExpression.ofColumn(ColumnType.STRING, column);
  }

  /**
   * @deprecated prefer to make {@link RobuxExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static RobuxExpression makeExpression(final String staticExpression)
  {
    return makeExpression(ColumnType.STRING, staticExpression);
  }

  /**
   * @deprecated prefer to make {@link RobuxExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static RobuxExpression makeExpression(final ColumnType columnType, final String staticExpression)
  {
    return makeExpression(columnType, null, staticExpression);
  }

  /**
   * @deprecated prefer to make {@link RobuxExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static RobuxExpression makeExpression(final SimpleExtraction simpleExtraction, final String staticExpression)
  {
    return makeExpression(ColumnType.STRING, simpleExtraction, staticExpression);
  }

  /**
   * @deprecated prefer to make {@link RobuxExpression} directly to ensure expression tests accurately test the full
   * expression structure, this method is just to have a convenient way to fix a very large number of existing tests
   */
  @Deprecated
  public static RobuxExpression makeExpression(
      final ColumnType columnType,
      final SimpleExtraction simpleExtraction,
      final String staticExpression
  )
  {
    return RobuxExpression.ofExpression(
        columnType,
        simpleExtraction,
        (args) -> staticExpression,
        Collections.emptyList()
    );
  }

  protected static ResourceAction viewRead(final String viewName)
  {
    return new ResourceAction(new Resource(viewName, ResourceType.VIEW), Action.READ);
  }

  protected static ResourceAction dataSourceRead(final String dataSource)
  {
    return new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.READ);
  }

  protected static ResourceAction dataSourceWrite(final String dataSource)
  {
    return new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE);
  }

  protected static ResourceAction externalRead(final String inputSourceType)
  {
    return new ResourceAction(new Resource(inputSourceType, ResourceType.EXTERNAL), Action.READ);
  }

  protected static ResourceAction externalWrite(final String inputSourceType)
  {
    return new ResourceAction(new Resource(inputSourceType, ResourceType.EXTERNAL), Action.WRITE);
  }
}

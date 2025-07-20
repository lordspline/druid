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

package org.apache.robux.query.sql;

import com.google.common.collect.ImmutableList;
import org.apache.robux.guice.SleepModule;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.query.scan.ScanQuery.ResultFormat;
import org.apache.robux.query.sql.SleepSqlTest.SleepComponentSupplier;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.virtual.ExpressionVirtualColumn;
import org.apache.robux.sql.calcite.BaseCalciteQueryTest;
import org.apache.robux.sql.calcite.SqlTestFrameworkConfig;
import org.apache.robux.sql.calcite.TempDirProducer;
import org.apache.robux.sql.calcite.filtration.Filtration;
import org.apache.robux.sql.calcite.util.RobuxModuleCollection;
import org.apache.robux.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.junit.jupiter.api.Test;

@SqlTestFrameworkConfig.ComponentSupplier(SleepComponentSupplier.class)
public class SleepSqlTest extends BaseCalciteQueryTest
{
  public static class SleepComponentSupplier extends StandardComponentSupplier
  {
    public SleepComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public RobuxModule getCoreModule()
    {
      return RobuxModuleCollection.of(super.getCoreModule(), new SleepModule());
    }
  }

  @Test
  public void testSleepFunction()
  {
    testQuery(
        "SELECT sleep(m1) from foo where m1 < 2.0",
        ImmutableList.of(
            Robuxs.newScanQueryBuilder()
                  .dataSource(new TableDataSource("foo"))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "sleep(\"m1\")",
                          ColumnType.STRING,
                          queryFramework().macroTable()
                      )
                  )
                  .columns("v0")
                  .columnTypes(ColumnType.STRING)
                  .filters(range("m1", ColumnType.DOUBLE, null, 2.0, false, true))
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{null}
        )
    );
  }
}

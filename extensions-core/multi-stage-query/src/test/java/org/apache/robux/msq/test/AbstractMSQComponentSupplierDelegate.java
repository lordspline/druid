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

package org.apache.robux.msq.test;

import org.apache.robux.compressedbigdecimal.CompressedBigDecimalModule;
import org.apache.robux.guice.IndexingServiceTuningConfigModule;
import org.apache.robux.guice.JoinableFactoryModule;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.msq.exec.TestMSQSqlModule;
import org.apache.robux.msq.guice.MSQExternalDataSourceModule;
import org.apache.robux.msq.guice.MSQIndexingModule;
import org.apache.robux.msq.sql.MSQTaskSqlEngine;
import org.apache.robux.msq.test.CalciteMSQTestsHelper.MSQTestModule;
import org.apache.robux.sql.calcite.run.SqlEngine;
import org.apache.robux.sql.calcite.util.RobuxModuleCollection;
import org.apache.robux.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.robux.sql.calcite.util.SqlTestFramework.QueryComponentSupplierDelegate;

/**
 * Upgrade an existing QueryComponentSupplier to support MSQ tests.
 */
public class AbstractMSQComponentSupplierDelegate extends QueryComponentSupplierDelegate
{

  public AbstractMSQComponentSupplierDelegate(QueryComponentSupplier delegate)
  {
    super(delegate);
  }

  @Override
  public RobuxModule getCoreModule()
  {
    return RobuxModuleCollection.of(
        super.getCoreModule(),
        new MSQTestModule(),
        new IndexingServiceTuningConfigModule(),
        new JoinableFactoryModule(),
        new MSQExternalDataSourceModule(),
        new MSQIndexingModule(),
        new TestMSQSqlModule(),
        new CompressedBigDecimalModule()
    );
  }

  @Override
  public Class<? extends SqlEngine> getSqlEngineClass()
  {
    return MSQTaskSqlEngine.class;
  }

  @Override
  public Boolean isExplainSupported()
  {
    return false;
  }

}

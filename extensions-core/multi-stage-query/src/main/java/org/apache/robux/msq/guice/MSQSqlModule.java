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

package org.apache.robux.msq.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.annotations.LoadScope;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.metadata.input.InputSourceModule;
import org.apache.robux.msq.indexing.destination.MSQTerminalStageSpecFactory;
import org.apache.robux.msq.indexing.destination.SegmentGenerationTerminalStageSpecFactory;
import org.apache.robux.msq.sql.MSQTaskSqlEngine;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.SqlToolbox;
import org.apache.robux.sql.calcite.external.ExternalOperatorConversion;
import org.apache.robux.sql.guice.SqlBindings;

import java.util.List;

/**
 * Module for providing the {@code EXTERN} operator.
 */
@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class MSQSqlModule implements RobuxModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    // We want this module to bring input sources along for the ride.
    return new InputSourceModule().getJacksonModules();
  }

  @Override
  public void configure(Binder binder)
  {
    // We want this module to bring InputSourceModule along for the ride.
    binder.install(new InputSourceModule());

    binder.bind(MSQTerminalStageSpecFactory.class).to(SegmentGenerationTerminalStageSpecFactory.class).in(LazySingleton.class);

    binder.bind(MSQTaskSqlEngine.class).in(LazySingleton.class);

    // Set up the EXTERN macro.
    SqlBindings.addOperatorConversion(binder, ExternalOperatorConversion.class);
  }

  @Provides
  @MultiStageQuery
  @LazySingleton
  public SqlStatementFactory makeMSQSqlStatementFactory(
      final MSQTaskSqlEngine engine,
      final SqlToolbox toolbox
  )
  {
    return new SqlStatementFactory(toolbox.withEngine(engine));
  }
}

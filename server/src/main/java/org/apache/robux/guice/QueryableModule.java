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

package org.apache.robux.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.server.log.ComposingRequestLoggerProvider;
import org.apache.robux.server.log.EmittingRequestLoggerProvider;
import org.apache.robux.server.log.FileRequestLoggerProvider;
import org.apache.robux.server.log.FilteredRequestLoggerProvider;
import org.apache.robux.server.log.LoggingRequestLoggerProvider;
import org.apache.robux.server.log.NoopRequestLoggerProvider;
import org.apache.robux.server.log.RequestLogger;
import org.apache.robux.server.log.RequestLoggerProvider;
import org.apache.robux.server.log.SwitchingRequestLoggerProvider;

import java.util.Collections;
import java.util.List;

/**
 */
public class QueryableModule implements RobuxModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(RequestLogger.class).toProvider(RequestLoggerProvider.class).in(ManageLifecycle.class);
    JsonConfigProvider.bindWithDefault(
        binder,
        "robux.request.logging",
        RequestLoggerProvider.class,
        NoopRequestLoggerProvider.class
    );

    binder.bind(QueryRunnerFactoryConglomerate.class)
          .to(DefaultQueryRunnerFactoryConglomerate.class)
          .in(LazySingleton.class);
  }

  @Override
  public List<Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("QueryableModule")
            .registerSubtypes(
                NoopRequestLoggerProvider.class,
                EmittingRequestLoggerProvider.class,
                FileRequestLoggerProvider.class,
                LoggingRequestLoggerProvider.class,
                ComposingRequestLoggerProvider.class,
                FilteredRequestLoggerProvider.class,
                SwitchingRequestLoggerProvider.class
            )
    );
  }
}

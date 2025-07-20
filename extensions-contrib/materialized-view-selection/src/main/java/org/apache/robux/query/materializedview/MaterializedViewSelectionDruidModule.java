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

package org.apache.robux.query.materializedview;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Singleton;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.RobuxBinders;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.annotations.LoadScope;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.server.metrics.MetricsModule;

import java.util.List;

@LoadScope(roles = NodeRole.BROKER_JSON_NAME)
public class MaterializedViewSelectionRobuxModule implements RobuxModule
{
  @Override 
  public List<? extends Module> getJacksonModules() 
  {
    return ImmutableList.of(
        new SimpleModule(getClass().getSimpleName())
            .registerSubtypes(
                new NamedType(MaterializedViewQuery.class, MaterializedViewQuery.TYPE))
    );
  }
  
  @Override
  public void configure(Binder binder) 
  {
    RobuxBinders.queryToolChestBinder(binder)
        .addBinding(MaterializedViewQuery.class)
        .to(MaterializedViewQueryQueryToolChest.class);
    LifecycleModule.register(binder, DerivativeDataSourceManager.class);
    binder.bind(DataSourceOptimizer.class).in(Singleton.class);
    MetricsModule.register(binder, DataSourceOptimizerMonitor.class);
    JsonConfigProvider.bind(binder, "robux.manager.derivatives", MaterializedViewConfig.class);
  }
}

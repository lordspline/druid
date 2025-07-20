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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import org.apache.robux.client.InternalQueryConfig;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.query.DefaultGenericQueryMetricsFactory;
import org.apache.robux.query.DefaultQueryConfig;
import org.apache.robux.query.GenericQueryMetricsFactory;
import org.apache.robux.query.MapQueryToolChestWarehouse;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryRunnerFactory;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.QueryToolChest;
import org.apache.robux.query.QueryToolChestWarehouse;
import org.apache.robux.query.QueryWatcher;
import org.apache.robux.query.RetryQueryRunnerConfig;
import org.apache.robux.query.metadata.SegmentMetadataQueryConfig;
import org.apache.robux.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.robux.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.robux.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.segment.metadata.CoordinatorSegmentMetadataCache;
import org.apache.robux.segment.metadata.SegmentMetadataQuerySegmentWalker;
import org.apache.robux.server.QueryScheduler;
import org.apache.robux.server.QuerySchedulerProvider;

/**
 * Module that binds dependencies required for segment schema management and
 * caching on the Coordinator.
 *
 * @see CentralizedDatasourceSchemaConfig
 * @see CoordinatorSegmentMetadataCache
 */
public class SegmentSchemaCacheModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "robux.coordinator.segmentMetadata", SegmentMetadataQueryConfig.class);
    JsonConfigProvider.bind(binder, "robux.coordinator.query.scheduler", QuerySchedulerProvider.class, Global.class);
    JsonConfigProvider.bind(binder, "robux.coordinator.query.default", DefaultQueryConfig.class);
    JsonConfigProvider.bind(binder, "robux.coordinator.query.retryPolicy", RetryQueryRunnerConfig.class);
    JsonConfigProvider.bind(binder, "robux.coordinator.internal.query.config", InternalQueryConfig.class);

    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = RobuxBinders.queryToolChestBinder(binder);
    toolChests.addBinding(SegmentMetadataQuery.class).to(SegmentMetadataQueryQueryToolChest.class);
    binder.bind(SegmentMetadataQueryQueryToolChest.class).in(LazySingleton.class);
    binder.bind(QueryToolChestWarehouse.class).to(MapQueryToolChestWarehouse.class);

    final MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder =
        RobuxBinders.queryRunnerFactoryBinder(binder);
    queryFactoryBinder.addBinding(SegmentMetadataQuery.class).to(SegmentMetadataQueryRunnerFactory.class);
    RobuxBinders.queryBinder(binder);
    binder.bind(SegmentMetadataQueryRunnerFactory.class).in(LazySingleton.class);

    binder.bind(GenericQueryMetricsFactory.class).to(DefaultGenericQueryMetricsFactory.class);

    binder.bind(QueryScheduler.class)
          .toProvider(Key.get(QuerySchedulerProvider.class, Global.class))
          .in(LazySingleton.class);
    binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
    binder.bind(QuerySegmentWalker.class).to(SegmentMetadataQuerySegmentWalker.class).in(LazySingleton.class);

    LifecycleModule.register(binder, CoordinatorSegmentMetadataCache.class);
  }

  @LazySingleton
  @Provides
  public QueryWatcher getWatcher(QueryScheduler scheduler)
  {
    return scheduler;
  }
}

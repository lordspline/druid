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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryRunnerFactory;
import org.apache.robux.query.QueryWatcher;
import org.apache.robux.query.datasourcemetadata.DataSourceMetadataQuery;
import org.apache.robux.query.datasourcemetadata.DataSourceMetadataQueryRunnerFactory;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.query.groupby.GroupByQueryRunnerFactory;
import org.apache.robux.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.robux.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.robux.query.operator.WindowOperatorQuery;
import org.apache.robux.query.operator.WindowOperatorQueryQueryRunnerFactory;
import org.apache.robux.query.scan.ScanQuery;
import org.apache.robux.query.scan.ScanQueryRunnerFactory;
import org.apache.robux.query.search.SearchQuery;
import org.apache.robux.query.search.SearchQueryRunnerFactory;
import org.apache.robux.query.timeboundary.TimeBoundaryQuery;
import org.apache.robux.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import org.apache.robux.query.timeseries.TimeseriesQuery;
import org.apache.robux.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.robux.query.topn.TopNQuery;
import org.apache.robux.query.topn.TopNQueryRunnerFactory;
import org.apache.robux.query.union.UnionQuery;
import org.apache.robux.query.union.UnionQueryLogic;
import org.apache.robux.server.QueryScheduler;
import org.apache.robux.server.QuerySchedulerProvider;

import java.util.Map;

/**
 */
public class QueryRunnerFactoryModule extends QueryToolChestModule
{
  private static final Map<Class<? extends Query<?>>, Class<? extends QueryRunnerFactory<?, ?>>> MAPPINGS =
      ImmutableMap.<Class<? extends Query<?>>, Class<? extends QueryRunnerFactory<?, ?>>>builder()
                  .put(DataSourceMetadataQuery.class, DataSourceMetadataQueryRunnerFactory.class)
                  .put(GroupByQuery.class, GroupByQueryRunnerFactory.class)
                  .put(ScanQuery.class, ScanQueryRunnerFactory.class)
                  .put(SearchQuery.class, SearchQueryRunnerFactory.class)
                  .put(SegmentMetadataQuery.class, SegmentMetadataQueryRunnerFactory.class)
                  .put(TimeBoundaryQuery.class, TimeBoundaryQueryRunnerFactory.class)
                  .put(TimeseriesQuery.class, TimeseriesQueryRunnerFactory.class)
                  .put(TopNQuery.class, TopNQueryRunnerFactory.class)
                  .put(WindowOperatorQuery.class, WindowOperatorQueryQueryRunnerFactory.class)
                  .build();

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    binder.bind(QueryScheduler.class)
          .toProvider(Key.get(QuerySchedulerProvider.class, Global.class))
          .in(LazySingleton.class);
    binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "robux.query.scheduler", QuerySchedulerProvider.class, Global.class);

    final MapBinder<Class<? extends Query>, QueryRunnerFactory> queryFactoryBinder = RobuxBinders.queryRunnerFactoryBinder(
        binder
    );

    for (Map.Entry<Class<? extends Query<?>>, Class<? extends QueryRunnerFactory<?, ?>>> entry : MAPPINGS.entrySet()) {
      queryFactoryBinder.addBinding(entry.getKey()).to(entry.getValue());
      binder.bind(entry.getValue()).in(LazySingleton.class);
    }

    RobuxBinders.queryBinder(binder)
        .bindQueryLogic(UnionQuery.class, UnionQueryLogic.class);
  }

  @LazySingleton
  @Provides
  public QueryWatcher getWatcher(QueryScheduler scheduler)
  {
    return scheduler;
  }
}

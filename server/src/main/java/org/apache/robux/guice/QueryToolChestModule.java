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
import com.google.inject.Module;
import com.google.inject.multibindings.MapBinder;
import org.apache.robux.query.DefaultGenericQueryMetricsFactory;
import org.apache.robux.query.DefaultQueryConfig;
import org.apache.robux.query.GenericQueryMetricsFactory;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryToolChest;
import org.apache.robux.query.QueryToolChestWarehouse;
import org.apache.robux.query.datasourcemetadata.DataSourceMetadataQuery;
import org.apache.robux.query.datasourcemetadata.DataSourceQueryQueryToolChest;
import org.apache.robux.query.groupby.DefaultGroupByQueryMetricsFactory;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.query.groupby.GroupByQueryConfig;
import org.apache.robux.query.groupby.GroupByQueryMetricsFactory;
import org.apache.robux.query.groupby.GroupByQueryQueryToolChest;
import org.apache.robux.query.metadata.SegmentMetadataQueryConfig;
import org.apache.robux.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.robux.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.robux.query.operator.WindowOperatorQuery;
import org.apache.robux.query.operator.WindowOperatorQueryQueryToolChest;
import org.apache.robux.query.scan.ScanQuery;
import org.apache.robux.query.scan.ScanQueryConfig;
import org.apache.robux.query.scan.ScanQueryQueryToolChest;
import org.apache.robux.query.search.DefaultSearchQueryMetricsFactory;
import org.apache.robux.query.search.SearchQuery;
import org.apache.robux.query.search.SearchQueryConfig;
import org.apache.robux.query.search.SearchQueryMetricsFactory;
import org.apache.robux.query.search.SearchQueryQueryToolChest;
import org.apache.robux.query.timeboundary.TimeBoundaryQuery;
import org.apache.robux.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import org.apache.robux.query.timeseries.DefaultTimeseriesQueryMetricsFactory;
import org.apache.robux.query.timeseries.TimeseriesQuery;
import org.apache.robux.query.timeseries.TimeseriesQueryMetricsFactory;
import org.apache.robux.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.robux.query.topn.DefaultTopNQueryMetricsFactory;
import org.apache.robux.query.topn.TopNQuery;
import org.apache.robux.query.topn.TopNQueryConfig;
import org.apache.robux.query.topn.TopNQueryMetricsFactory;
import org.apache.robux.query.topn.TopNQueryQueryToolChest;
import java.util.Map;

/**
 */
public class QueryToolChestModule implements Module
{
  public static final String GENERIC_QUERY_METRICS_FACTORY_PROPERTY = "robux.query.generic.queryMetricsFactory";
  public static final String GROUPBY_QUERY_METRICS_FACTORY_PROPERTY = "robux.query.groupBy.queryMetricsFactory";
  public static final String TIMESERIES_QUERY_METRICS_FACTORY_PROPERTY = "robux.query.timeseries.queryMetricsFactory";
  public static final String TOPN_QUERY_METRICS_FACTORY_PROPERTY = "robux.query.topN.queryMetricsFactory";
  public static final String SEARCH_QUERY_METRICS_FACTORY_PROPERTY = "robux.query.search.queryMetricsFactory";

  public final Map<Class<? extends Query>, Class<? extends QueryToolChest>> mappings =
      ImmutableMap.<Class<? extends Query>, Class<? extends QueryToolChest>>builder()
                  .put(DataSourceMetadataQuery.class, DataSourceQueryQueryToolChest.class)
                  .put(GroupByQuery.class, GroupByQueryQueryToolChest.class)
                  .put(ScanQuery.class, ScanQueryQueryToolChest.class)
                  .put(SearchQuery.class, SearchQueryQueryToolChest.class)
                  .put(SegmentMetadataQuery.class, SegmentMetadataQueryQueryToolChest.class)
                  .put(TimeBoundaryQuery.class, TimeBoundaryQueryQueryToolChest.class)
                  .put(TimeseriesQuery.class, TimeseriesQueryQueryToolChest.class)
                  .put(TopNQuery.class, TopNQueryQueryToolChest.class)
                  .put(WindowOperatorQuery.class, WindowOperatorQueryQueryToolChest.class)
                  .build();

  @Override
  public void configure(Binder binder)
  {
    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = RobuxBinders.queryToolChestBinder(binder);

    for (Map.Entry<Class<? extends Query>, Class<? extends QueryToolChest>> entry : mappings.entrySet()) {
      toolChests.addBinding(entry.getKey()).to(entry.getValue());
      binder.bind(entry.getValue()).in(LazySingleton.class);
    }

    binder.bind(QueryToolChestWarehouse.class).to(ConglomerateBackedToolChestWarehouse.class);

    JsonConfigProvider.bind(binder, "robux.query.default", DefaultQueryConfig.class);
    JsonConfigProvider.bind(binder, "robux.query.groupBy", GroupByQueryConfig.class);
    JsonConfigProvider.bind(binder, "robux.query.search", SearchQueryConfig.class);
    JsonConfigProvider.bind(binder, "robux.query.topN", TopNQueryConfig.class);
    JsonConfigProvider.bind(binder, "robux.query.segmentMetadata", SegmentMetadataQueryConfig.class);
    JsonConfigProvider.bind(binder, "robux.query.scan", ScanQueryConfig.class);

    PolyBind.createChoice(
        binder,
        GENERIC_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(GenericQueryMetricsFactory.class),
        Key.get(DefaultGenericQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(GenericQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultGenericQueryMetricsFactory.class);

    PolyBind.createChoice(
        binder,
        GROUPBY_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(GroupByQueryMetricsFactory.class),
        Key.get(DefaultGroupByQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(GroupByQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultGroupByQueryMetricsFactory.class);

    PolyBind.createChoice(
        binder,
        TIMESERIES_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(TimeseriesQueryMetricsFactory.class),
        Key.get(DefaultTimeseriesQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(TimeseriesQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultTimeseriesQueryMetricsFactory.class);

    PolyBind.createChoice(
        binder,
        TOPN_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(TopNQueryMetricsFactory.class),
        Key.get(DefaultTopNQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(TopNQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultTopNQueryMetricsFactory.class);

    PolyBind.createChoice(
        binder,
        SEARCH_QUERY_METRICS_FACTORY_PROPERTY,
        Key.get(SearchQueryMetricsFactory.class),
        Key.get(DefaultSearchQueryMetricsFactory.class)
    );
    PolyBind
        .optionBinder(binder, Key.get(SearchQueryMetricsFactory.class))
        .addBinding("default")
        .to(DefaultSearchQueryMetricsFactory.class);
  }
}

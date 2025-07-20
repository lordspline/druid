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

package org.apache.robux.query.movingaverage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import org.apache.robux.client.CachingClusteredClient;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.ImmutableRobuxServer;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.client.cache.CachePopulatorStats;
import org.apache.robux.client.cache.ForegroundCachePopulator;
import org.apache.robux.client.cache.MapCache;
import org.apache.robux.client.selector.ServerSelector;
import org.apache.robux.data.input.MapBasedRow;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.RobuxProcessingModule;
import org.apache.robux.guice.GuiceInjectors;
import org.apache.robux.guice.QueryRunnerFactoryModule;
import org.apache.robux.guice.QueryableModule;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.guice.http.RobuxHttpClientConfig;
import org.apache.robux.initialization.Initialization;
import org.apache.robux.java.util.common.guava.Accumulators;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.guava.Sequences;
import org.apache.robux.query.BrokerParallelMergeConfig;
import org.apache.robux.query.DefaultGenericQueryMetricsFactory;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.Result;
import org.apache.robux.query.RetryQueryRunnerConfig;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.query.groupby.GroupByQuery;
import org.apache.robux.query.groupby.ResultRow;
import org.apache.robux.query.movingaverage.test.TestConfig;
import org.apache.robux.query.timeseries.TimeseriesQuery;
import org.apache.robux.query.timeseries.TimeseriesResultValue;
import org.apache.robux.segment.join.MapJoinableFactory;
import org.apache.robux.server.ClientQuerySegmentWalker;
import org.apache.robux.server.QueryStackTests;
import org.apache.robux.server.SubqueryGuardrailHelper;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.server.metrics.SubqueryCountStatsProvider;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.apache.robux.timeline.TimelineLookup;
import org.apache.robux.utils.JvmUtils;
import org.hamcrest.core.IsInstanceOf;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * Base class for implementing MovingAverageQuery tests
 */
@RunWith(Parameterized.class)
public class MovingAverageQueryTest extends InitializedNullHandlingTest
{
  private final ObjectMapper jsonMapper;
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final RetryQueryRunnerConfig retryConfig;
  private final ServerConfig serverConfig;

  private final List<ResultRow> groupByResults = new ArrayList<>();
  private final List<Result<TimeseriesResultValue>> timeseriesResults = new ArrayList<>();

  private final TestConfig config;

  @Parameters(name = "{0}")
  public static Iterable<String[]> data() throws IOException
  {
    BufferedReader testReader = new BufferedReader(
        new InputStreamReader(MovingAverageQueryTest.class.getResourceAsStream("/queryTests"), StandardCharsets.UTF_8));
    List<String[]> tests = new ArrayList<>();

    for (String line = testReader.readLine(); line != null; line = testReader.readLine()) {
      tests.add(new String[]{line});
    }

    return tests;
  }

  public MovingAverageQueryTest(String yamlFile) throws IOException
  {

    List<Module> modules = getRequiredModules();
    modules.add(
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("queryTest");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(1);
          binder.bind(QuerySegmentWalker.class).toProvider(Providers.of(new QuerySegmentWalker()
          {
            @Override
            public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
            {
              return (queryPlus, responseContext) -> {
                if (query instanceof GroupByQuery) {
                  return (Sequence<T>) Sequences.simple(groupByResults);
                } else if (query instanceof TimeseriesQuery) {
                  return (Sequence<T>) Sequences.simple(timeseriesResults);
                }
                throw new UnsupportedOperationException("unexpected query type " + query.getType());
              };
            }

            @Override
            public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
            {
              return getQueryRunnerForIntervals(query, null);
            }
          }));
          Multibinder.newSetBinder(binder, NodeRole.class, Self.class).addBinding().toInstance(NodeRole.BROKER);
        }
    );

    System.setProperty("robux.processing.buffer.sizeBytes", "655360");
    Injector baseInjector = GuiceInjectors.makeStartupInjector();
    Injector injector = Initialization.makeInjectorWithModules(baseInjector, modules);

    jsonMapper = injector.getInstance(ObjectMapper.class);
    conglomerate = injector.getInstance(QueryRunnerFactoryConglomerate.class);
    retryConfig = injector.getInstance(RetryQueryRunnerConfig.class);
    serverConfig = injector.getInstance(ServerConfig.class);

    InputStream is = getClass().getResourceAsStream("/queryTests/" + yamlFile);
    ObjectMapper reader = new ObjectMapper(new YAMLFactory());
    config = reader.readValue(is, TestConfig.class);
  }

  /**
   * Returns the JSON query that should be used in the test.
   *
   * @return The JSON query
   */
  private String getQueryString()
  {
    return config.query.toString();
  }

  /**
   * Returns the JSON result that should be expected from the query.
   *
   * @return The JSON result
   */
  private String getExpectedResultString()
  {
    return config.expectedOutput.toString();
  }

  /**
   * Returns the JSON result that the nested groupby query should produce.
   * Either this method or {@link #getTimeseriesResultJson()} must be defined
   * by the subclass.
   *
   * @return The JSON result from the groupby query
   */
  private String getGroupByResultJson()
  {
    ArrayNode node = config.intermediateResults.get("groupBy");
    return node == null ? null : node.toString();
  }

  /**
   * Returns the JSON result that the nested timeseries query should produce.
   * Either this method or {@link #getGroupByResultJson()} must be defined
   * by the subclass.
   *
   * @return The JSON result from the timeseries query
   */
  private String getTimeseriesResultJson()
  {
    ArrayNode node = config.intermediateResults.get("timeseries");
    return node == null ? null : node.toString();
  }

  /**
   * Returns the expected query type.
   *
   * @return The Query type
   */
  private Class<?> getExpectedQueryType()
  {
    return MovingAverageQuery.class;
  }

  private TypeReference<List<MapBasedRow>> getExpectedResultType()
  {
    return new TypeReference<>() {};
  }

  /**
   * Returns a list of any additional Robux Modules necessary to run the test.
   */
  private List<Module> getRequiredModules()
  {
    List<Module> list = new ArrayList<>();

    list.add(new QueryRunnerFactoryModule());
    list.add(new QueryableModule());
    list.add(new RobuxProcessingModule());

    return list;
  }

  /**
   * Set up any needed mocks to stub out backend query behavior.
   */
  private void defineMocks() throws IOException
  {
    groupByResults.clear();
    timeseriesResults.clear();

    if (getGroupByResultJson() != null) {
      groupByResults.addAll(jsonMapper.readValue(getGroupByResultJson(), new TypeReference<List<ResultRow>>() {}));
    }

    if (getTimeseriesResultJson() != null) {
      timeseriesResults.addAll(
          jsonMapper.readValue(
              getTimeseriesResultJson(),
              new TypeReference<List<Result<TimeseriesResultValue>>>() {}
          )
      );
    }
  }

  /**
   * converts Int to Long, Float to Double in the actual and expected result
   */
  private List<MapBasedRow> consistentTypeCasting(List<MapBasedRow> result)
  {
    List<MapBasedRow> newResult = new ArrayList<>();
    for (MapBasedRow row : result) {
      final Map<String, Object> event = Maps.newLinkedHashMap((row).getEvent());
      event.forEach((key, value) -> {
        if (value instanceof Integer) {
          event.put(key, ((Integer) value).longValue());
        }
        if (value instanceof Float) {
          event.put(key, ((Float) value).doubleValue());
        }
      });
      newResult.add(new MapBasedRow(row.getTimestamp(), event));
    }

    return newResult;
  }

  /**
   * Validate that the specified query behaves correctly.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testQuery() throws IOException
  {
    Query<?> query = jsonMapper.readValue(getQueryString(), Query.class);
    Assert.assertThat(query, IsInstanceOf.instanceOf(getExpectedQueryType()));

    List<MapBasedRow> expectedResults = jsonMapper.readValue(getExpectedResultString(), getExpectedResultType());
    Assert.assertNotNull(expectedResults);
    Assert.assertThat(expectedResults, IsInstanceOf.instanceOf(List.class));

    RobuxHttpClientConfig httpClientConfig = new RobuxHttpClientConfig()
    {
      @Override
      public long getMaxQueuedBytes()
      {
        return 0L;
      }
    };
    CachingClusteredClient baseClient = new CachingClusteredClient(
        conglomerate,
        new TimelineServerView()
        {
          @Override
          public Optional<? extends TimelineLookup<String, ServerSelector>> getTimeline(TableDataSource analysis)
          {
            return Optional.empty();
          }

          @Override
          public List<ImmutableRobuxServer> getRobuxServers()
          {
            return null;
          }

          @Override
          public <T> QueryRunner<T> getQueryRunner(RobuxServer server)
          {
            return null;
          }

          @Override
          public void registerTimelineCallback(Executor exec, TimelineCallback callback)
          {

          }

          @Override
          public void registerSegmentCallback(Executor exec, SegmentCallback callback)
          {

          }

          @Override
          public void registerServerCallback(Executor exec, ServerCallback callback)
          {

          }
        },
        MapCache.create(100000),
        jsonMapper,
        new ForegroundCachePopulator(jsonMapper, new CachePopulatorStats(), -1),
        new CacheConfig(),
        httpClientConfig,
        new BrokerParallelMergeConfig(),
        ForkJoinPool.commonPool(),
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new NoopServiceEmitter()
    );

    ClientQuerySegmentWalker walker = new ClientQuerySegmentWalker(
        new NoopServiceEmitter(),
        baseClient,
        null /* local client; unused in this test, so pass in null */,
        conglomerate,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        retryConfig,
        jsonMapper,
        serverConfig,
        null,
        new CacheConfig(),
        new SubqueryGuardrailHelper(null, JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(), 1),
        new SubqueryCountStatsProvider(),
        new DefaultGenericQueryMetricsFactory()
    );

    defineMocks();

    QueryPlus queryPlus = QueryPlus.wrap(query);
    final Sequence<?> res = query.getRunner(walker).run(queryPlus);

    List actualResults = new ArrayList();
    actualResults = res.accumulate(actualResults, Accumulators.list());

    expectedResults = consistentTypeCasting(expectedResults);
    actualResults = consistentTypeCasting(actualResults);

    Assert.assertEquals(expectedResults, actualResults);
  }
}

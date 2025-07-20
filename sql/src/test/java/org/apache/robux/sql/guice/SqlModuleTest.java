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

package org.apache.robux.sql.guice;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.robux.client.FilteredServerInventoryView;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.client.coordinator.Coordinator;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.client.coordinator.NoopCoordinatorClient;
import org.apache.robux.client.indexing.IndexingService;
import org.apache.robux.discovery.RobuxLeaderClient;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.guice.RobuxGuiceExtensions;
import org.apache.robux.guice.JsonConfigurator;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.PolyBind;
import org.apache.robux.guice.ServerModule;
import org.apache.robux.guice.security.PolicyModule;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.jackson.JacksonModule;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.DefaultQueryConfig;
import org.apache.robux.query.GenericQueryMetricsFactory;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.QueryToolChestWarehouse;
import org.apache.robux.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.segment.join.JoinableFactory;
import org.apache.robux.segment.loading.SegmentCacheManager;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.server.QueryScheduler;
import org.apache.robux.server.QuerySchedulerProvider;
import org.apache.robux.server.ResponseContextConfig;
import org.apache.robux.server.initialization.AuthenticatorMapperModule;
import org.apache.robux.server.log.NoopRequestLogger;
import org.apache.robux.server.log.RequestLogger;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.server.security.Escalator;
import org.apache.robux.server.security.NoopEscalator;
import org.apache.robux.sql.calcite.planner.CatalogResolver;
import org.apache.robux.sql.calcite.planner.PlannerFactory;
import org.apache.robux.sql.calcite.util.CalciteTests;
import org.apache.robux.sql.calcite.view.RobuxViewMacro;
import org.apache.robux.sql.calcite.view.NoopViewManager;
import org.apache.robux.sql.calcite.view.ViewManager;
import org.apache.robux.sql.http.SqlResourceTest;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Map;
import java.util.Properties;

@RunWith(EasyMockRunner.class)
public class SqlModuleTest
{
  @Mock
  private ServiceEmitter serviceEmitter;

  @Mock
  private FilteredServerInventoryView inventoryView;

  @Mock
  private TimelineServerView timelineServerView;

  @Mock
  private RobuxLeaderClient robuxLeaderClient;

  @Mock
  private RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider;

  @Mock
  private GenericQueryMetricsFactory genericQueryMetricsFactory;

  @Mock
  private QuerySegmentWalker querySegmentWalker;

  @Mock
  private QueryToolChestWarehouse queryToolChestWarehouse;

  @Mock
  private LookupExtractorFactoryContainerProvider lookupExtractorFactoryContainerProvider;

  @Mock
  private JoinableFactory joinableFactory;

  @Mock
  private SegmentCacheManager segmentCacheManager;

  @Mock
  private QueryRunnerFactoryConglomerate conglomerate;

  private Injector injector;

  @Before
  public void setUp()
  {
    EasyMock.replay(
        serviceEmitter,
        inventoryView,
        timelineServerView,
        robuxLeaderClient,
        robuxNodeDiscoveryProvider,
        genericQueryMetricsFactory,
        querySegmentWalker,
        queryToolChestWarehouse,
        lookupExtractorFactoryContainerProvider,
        joinableFactory,
        segmentCacheManager
    );
  }

  @Test
  public void testDefaultViewManagerBind()
  {
    final Properties props = new Properties();
    props.setProperty(SqlModule.PROPERTY_SQL_ENABLE, "true");
    props.setProperty(SqlModule.PROPERTY_SQL_ENABLE_AVATICA, "true");
    props.setProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true");

    injector = makeInjectorWithProperties(props);

    ViewManager viewManager = injector.getInstance(Key.get(ViewManager.class));
    Assert.assertNotNull(viewManager);
    Assert.assertTrue(viewManager instanceof NoopViewManager);
  }

  @Test
  public void testNonDefaultViewManagerBind()
  {
    final Properties props = new Properties();
    props.setProperty(SqlModule.PROPERTY_SQL_ENABLE, "true");
    props.setProperty(SqlModule.PROPERTY_SQL_ENABLE_AVATICA, "true");
    props.setProperty(SqlModule.PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true");
    props.setProperty(SqlModule.PROPERTY_SQL_VIEW_MANAGER_TYPE, "bindtest");

    injector = makeInjectorWithProperties(props);

    ViewManager viewManager = injector.getInstance(Key.get(ViewManager.class));
    Assert.assertNotNull(viewManager);
    Assert.assertTrue(viewManager instanceof BindTestViewManager);
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    final SqlModule sqlModule = new SqlModule();
    sqlModule.setProps(props);

    return Guice.createInjector(
        ImmutableList.of(
            new RobuxGuiceExtensions(),
            new LifecycleModule(),
            new ServerModule(),
            new JacksonModule(),
            new PolicyModule(),
            new AuthenticatorMapperModule(),
            binder -> {
              binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
              binder.bind(JsonConfigurator.class).in(LazySingleton.class);
              binder.bind(Properties.class).toInstance(props);
              binder.bind(ExprMacroTable.class).toInstance(ExprMacroTable.nil());
              binder.bind(AuthorizerMapper.class).toInstance(CalciteTests.TEST_AUTHORIZER_MAPPER);
              binder.bind(Escalator.class).toInstance(new NoopEscalator());
              binder.bind(ServiceEmitter.class).toInstance(serviceEmitter);
              binder.bind(RequestLogger.class).toInstance(NoopRequestLogger.instance());
              binder.bind(new TypeLiteral<Supplier<DefaultQueryConfig>>(){}).toInstance(Suppliers.ofInstance(new DefaultQueryConfig(null)));
              binder.bind(FilteredServerInventoryView.class).toInstance(inventoryView);
              binder.bind(TimelineServerView.class).toInstance(timelineServerView);
              binder.bind(RobuxLeaderClient.class).annotatedWith(Coordinator.class).toInstance(robuxLeaderClient);
              binder.bind(RobuxLeaderClient.class).annotatedWith(IndexingService.class).toInstance(robuxLeaderClient);
              binder.bind(RobuxNodeDiscoveryProvider.class).toInstance(robuxNodeDiscoveryProvider);
              binder.bind(GenericQueryMetricsFactory.class).toInstance(genericQueryMetricsFactory);
              binder.bind(QuerySegmentWalker.class).toInstance(querySegmentWalker);
              binder.bind(QueryToolChestWarehouse.class).toInstance(queryToolChestWarehouse);
              binder.bind(QueryRunnerFactoryConglomerate.class).toInstance(conglomerate);
              binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupExtractorFactoryContainerProvider);
              binder.bind(JoinableFactory.class).toInstance(joinableFactory);
              binder.bind(SegmentCacheManager.class).toInstance(segmentCacheManager);
              binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
              binder.bind(QueryScheduler.class)
                    .toProvider(QuerySchedulerProvider.class)
                    .in(LazySingleton.class);
              binder.bind(ResponseContextConfig.class).toInstance(SqlResourceTest.TEST_RESPONSE_CONTEXT_CONFIG);
              binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
              binder.bind(OverlordClient.class).to(NoopOverlordClient.class);
              binder.bind(CoordinatorClient.class).to(NoopCoordinatorClient.class);
              binder.bind(CentralizedDatasourceSchemaConfig.class)
                    .toInstance(CentralizedDatasourceSchemaConfig.enabled(false));
            },
            sqlModule,
            new TestViewManagerModule()
        )
    );
  }

  private static class TestViewManagerModule implements RobuxModule
  {
    @Override
    public void configure(Binder binder)
    {
      PolyBind.optionBinder(binder, Key.get(ViewManager.class))
              .addBinding("bindtest")
              .to(BindTestViewManager.class)
              .in(LazySingleton.class);
    }
  }

  private static class BindTestViewManager implements ViewManager
  {
    @Override
    public void createView(
        PlannerFactory plannerFactory,
        String viewName,
        String viewSql
    )
    {
    }

    @Override
    public void alterView(
        PlannerFactory plannerFactory,
        String viewName,
        String viewSql
    )
    {
    }

    @Override
    public void dropView(String viewName)
    {
    }

    @Override
    public Map<String, RobuxViewMacro> getViews()
    {
      return null;
    }
  }
}

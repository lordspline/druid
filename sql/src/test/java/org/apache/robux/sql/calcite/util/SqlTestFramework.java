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

package org.apache.robux.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.robux.client.TestHttpClient;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.client.cache.Cache;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.collections.BlockingPool;
import org.apache.robux.collections.NonBlockingPool;
import org.apache.robux.guice.BuiltInTypesModule;
import org.apache.robux.guice.RobuxInjectorBuilder;
import org.apache.robux.guice.ExpressionModule;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.QueryRunnerFactoryModule;
import org.apache.robux.guice.QueryableModule;
import org.apache.robux.guice.SegmentWranglerModule;
import org.apache.robux.guice.ServerModule;
import org.apache.robux.guice.StartupInjectorBuilder;
import org.apache.robux.guice.StorageNodeModule;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.guice.annotations.Merging;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.guice.security.PolicyModule;
import org.apache.robux.initialization.CoreInjectorBuilder;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.initialization.ServiceInjectorBuilder;
import org.apache.robux.java.util.common.RE;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.DefaultGenericQueryMetricsFactory;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.query.GlobalTableDataSource;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QueryRunnerTestHelper;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.QueryWatcher;
import org.apache.robux.query.RetryQueryRunnerConfig;
import org.apache.robux.query.TestBufferPool;
import org.apache.robux.query.groupby.DefaultGroupByQueryMetricsFactory;
import org.apache.robux.query.groupby.GroupByQueryConfig;
import org.apache.robux.query.groupby.GroupByQueryMetricsFactory;
import org.apache.robux.query.groupby.GroupByResourcesReservationPool;
import org.apache.robux.query.groupby.GroupByStatsProvider;
import org.apache.robux.query.groupby.GroupingEngine;
import org.apache.robux.query.groupby.TestGroupByBuffers;
import org.apache.robux.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.query.topn.TopNQueryConfig;
import org.apache.robux.quidem.ProjectPathUtils;
import org.apache.robux.quidem.TestSqlModule;
import org.apache.robux.segment.join.JoinableFactoryWrapper;
import org.apache.robux.segment.realtime.ChatHandlerProvider;
import org.apache.robux.segment.realtime.NoopChatHandlerProvider;
import org.apache.robux.server.ClientQuerySegmentWalker;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.LocalQuerySegmentWalker;
import org.apache.robux.server.QueryLifecycle;
import org.apache.robux.server.QueryLifecycleFactory;
import org.apache.robux.server.QueryStackTests;
import org.apache.robux.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.robux.server.SubqueryGuardrailHelper;
import org.apache.robux.server.TestClusterQuerySegmentWalker;
import org.apache.robux.server.TestClusterQuerySegmentWalker.TestSegmentsBroker;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.server.log.RequestLogger;
import org.apache.robux.server.log.TestRequestLogger;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.server.metrics.SubqueryCountStatsProvider;
import org.apache.robux.server.security.AuthConfig;
import org.apache.robux.server.security.AuthTestUtils;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.calcite.SqlTestFrameworkConfig;
import org.apache.robux.sql.calcite.TempDirProducer;
import org.apache.robux.sql.calcite.planner.CalciteRulesManager;
import org.apache.robux.sql.calcite.planner.CatalogResolver;
import org.apache.robux.sql.calcite.planner.RobuxOperatorTable;
import org.apache.robux.sql.calcite.planner.PlannerConfig;
import org.apache.robux.sql.calcite.planner.PlannerFactory;
import org.apache.robux.sql.calcite.rule.ExtensionCalciteRuleProvider;
import org.apache.robux.sql.calcite.run.NativeSqlEngine;
import org.apache.robux.sql.calcite.run.SqlEngine;
import org.apache.robux.sql.calcite.schema.RobuxSchema;
import org.apache.robux.sql.calcite.schema.RobuxSchemaCatalog;
import org.apache.robux.sql.calcite.schema.RobuxSchemaManager;
import org.apache.robux.sql.calcite.schema.LookupSchema;
import org.apache.robux.sql.calcite.schema.NoopRobuxSchemaManager;
import org.apache.robux.sql.calcite.schema.SystemSchema;
import org.apache.robux.sql.calcite.util.datasets.TestDataSet;
import org.apache.robux.sql.calcite.view.RobuxViewMacroFactory;
import org.apache.robux.sql.calcite.view.InProcessViewManager;
import org.apache.robux.sql.calcite.view.ViewManager;
import org.apache.robux.sql.guice.SqlModule;
import org.apache.robux.sql.hook.RobuxHookDispatcher;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.utils.JvmUtils;

import javax.inject.Named;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Builds the infrastructure needed to run Calcite tests. Building splits into
 * two parts: a constant part and a per-test part. The constant part includes
 * the congolmerate and walker (that is, the implementation of the test data),
 * while the per-query part includes the schema, planner factory and SQL
 * statement factory. The key reason for the split is that the per-test part
 * depends on the value of the {@link PlannerConfig} object, which varies between
 * tests.
 * <p>
 * The builder accepts the injector to use. "Calcite tests" use the injector
 * defined in {@link CalciteTests#INJECTOR}, while other tests can pass in the
 * preferred injector.
 * <p>
 * The framework allows the test to customize many of the framework components.
 * Since those components tend to depend on other framework components, we use
 * an indirection, {@link SqlTestFramework.QueryComponentSupplier QueryComponentSupplier}
 * to build those components. The methods of this interface match the methods
 * in @link org.apache.robux.sql.calcite.BaseCalciteQueryTest BaseCalciteQueryTest}
 * so that those tests can customize the framework just by overriding methods.
 * (This was done to maintain backward compatibility with earlier versions of the
 * code.) Other tests can implement {@code QueryComponentSupplier} directly by
 * extending {@link SqlTestFramework.StandardComponentSupplier StandardComponentSupplier}.
 * <p>
 * The framework should be built once per test class (not once per test method.)
 * Then, for each planner setup, call
 * {@link #plannerFixture(PlannerConfig, AuthConfig)}
 * to get a {@link PlannerFixture} with a view manager and planner factory. Call
 * {@link PlannerFixture#statementFactory()} to
 * obtain a the test-specific planner and wrapper classes for that test. After
 * that, tests use the various SQL statement classes to run tests. For tests
 * based on {@code BaseCalciteQueryTest}, the statements are wrapped by the
 * various {@code testQuery()} methods.
 * <p>
 * For tests that use non-standard views, first create the {@code PlannerFixture},
 * populate the views, then use the {@code QueryTestBuilder} directly, passing in
 * the {@code PlannerFixture} with views populated.
 * <p>
 * The framework holds on to the framework components. You can obtain the injector,
 * object mapper and other items by calling the various methods. The objects
 * are those created by the provided injector, or in this class, using objects
 * from that injector.
 */
public class SqlTestFramework
{
  /**
   * Interface to provide various framework components. Extend to customize,
   * use {@link StandardComponentSupplier} for the "standard" components.
   * <p>
   * Note that the methods here are named to match methods that already
   * exist in {@code BaseCalciteQueryTest}. Any changes here will impact that
   * base class, and possibly many test cases that extend that class.
   */
  public interface QueryComponentSupplier extends Closeable
  {
    /**
     * Gather properties to be used within tests. Particularly useful when choosing
     * among aggregator implementations: avoids the need to copy/paste code to select
     * the desired implementation.
     */
    void gatherProperties(Properties properties);

    Class<? extends SqlEngine> getSqlEngineClass();

    SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker);

    /**
     * Should return a module which provides the core Robux components.
     */
    RobuxModule getCoreModule();

    /**
     * Provides the overrides the core Robux components.
     */
    RobuxModule getOverrideModule();

    default CatalogResolver createCatalogResolver()
    {
      return CatalogResolver.NULL_RESOLVER;
    }

    /**
     * Configure the JSON mapper.
     */
    @Deprecated
    default void configureJsonMapper(ObjectMapper mapper)
    {
    }

    JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider);

    void finalizeTestFramework(SqlTestFramework sqlTestFramework);

    PlannerComponentSupplier getPlannerComponentSupplier();

    @Override
    default void close() throws IOException
    {
    }

    /**
     * Configures modules and overrides.
     *
     * New classes should use the {@link QueryComponentSupplier#getCoreModule()}
     * and {@link QueryComponentSupplier#getOverrideModule()} methods.
     */
    @Deprecated
    void configureGuice(RobuxInjectorBuilder injectorBuilder, List<Module> overrideModules);

    /**
     * Communicates if explain are supported.
     *
     * MSQ right now needs a full query run.
     */
    Boolean isExplainSupported();

    QueryRunnerFactoryConglomerate wrapConglomerate(QueryRunnerFactoryConglomerate conglomerate, Closer resourceCloser);

    TempDirProducer getTempDirProducer();
  }

  public abstract static class QueryComponentSupplierDelegate implements QueryComponentSupplier
  {
    private final QueryComponentSupplier delegate;

    public QueryComponentSupplierDelegate(QueryComponentSupplier delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public void gatherProperties(Properties properties)
    {
      delegate.gatherProperties(properties);
    }

    @Override
    public void configureGuice(RobuxInjectorBuilder builder, List<Module> overrideModules)
    {
      delegate.configureGuice(builder, overrideModules);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      return delegate.addSegmentsToWalker(walker);
    }

    @Override
    public void configureJsonMapper(ObjectMapper mapper)
    {
      delegate.configureJsonMapper(mapper);
    }

    @Override
    public JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider)
    {
      return delegate.createJoinableFactoryWrapper(lookupProvider);
    }

    @Override
    public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
    {
      delegate.finalizeTestFramework(sqlTestFramework);
    }

    @Override
    public PlannerComponentSupplier getPlannerComponentSupplier()
    {
      return delegate.getPlannerComponentSupplier();
    }

    @Override
    public void close() throws IOException
    {
      delegate.close();
    }

    @Override
    public Boolean isExplainSupported()
    {
      return delegate.isExplainSupported();
    }

    @Override
    public QueryRunnerFactoryConglomerate wrapConglomerate(
        QueryRunnerFactoryConglomerate conglomerate,
        Closer resourceCloser
    )
    {
      return delegate.wrapConglomerate(conglomerate, resourceCloser);
    }

    @Override
    public RobuxModule getCoreModule()
    {
      return delegate.getCoreModule();
    }

    @Override
    public RobuxModule getOverrideModule()
    {
      return delegate.getOverrideModule();
    }

    @Override
    public Class<? extends SqlEngine> getSqlEngineClass()
    {
      return delegate.getSqlEngineClass();
    }

    @Override
    public TempDirProducer getTempDirProducer()
    {
      return delegate.getTempDirProducer();
    }
  }

  public interface PlannerComponentSupplier
  {
    Set<ExtensionCalciteRuleProvider> extensionCalciteRules();

    ViewManager createViewManager();

    void populateViews(ViewManager viewManager, PlannerFactory plannerFactory);

    RobuxSchemaManager createSchemaManager();

    void finalizePlanner(PlannerFixture plannerFixture);
  }

  /**
   * Provides a "standard" set of query components, where "standard" just means
   * those you would get if you used {@code BaseCalciteQueryTest} with no
   * customization. {@code BaseCalciteQueryTest} uses this class to provide those
   * standard components.
   */
  public static class StandardComponentSupplier implements QueryComponentSupplier
  {
    protected final TempDirProducer tempDirProducer;
    private final PlannerComponentSupplier plannerComponentSupplier;

    public StandardComponentSupplier(
        final TempDirProducer tempDirProducer
    )
    {
      this.tempDirProducer = tempDirProducer;
      this.plannerComponentSupplier = buildPlannerComponentSupplier();
    }

    /**
     * Build the {@link PlannerComponentSupplier}.
     *
     * Implementations may override how this is being built.
     */
    protected PlannerComponentSupplier buildPlannerComponentSupplier()
    {
      return new StandardPlannerComponentSupplier();
    }

    @Override
    public void gatherProperties(Properties properties)
    {
    }

    @Override
    public RobuxModule getCoreModule()
    {
      return RobuxModuleCollection.of(
          new PolicyModule(),
          new LookylooModule(),
          new SegmentWranglerModule(),
          new ExpressionModule(),
          RobuxModule.override(
              new QueryRunnerFactoryModule(),
              new Module()
              {
                @Override
                public void configure(Binder binder)
                {

                }

                @Provides
                @Named("isExplainSupported")
                public Boolean isExplainSupported(Builder builder)
                {
                  return builder.componentSupplier.isExplainSupported();
                }

                @Provides
                public QueryComponentSupplier getQueryComponentSupplier(Builder builder)
                {
                  return builder.componentSupplier;
                }

                @Provides
                @LazySingleton
                GroupByQueryMetricsFactory groupByQueryMetricsFactory()
                {
                  return DefaultGroupByQueryMetricsFactory.instance();
                }

                @Provides
                @LazySingleton
                public TopNQueryConfig makeTopNQueryConfig(Builder builder)
                {
                  return new TopNQueryConfig()
                  {
                    @Override
                    public int getMinTopNThreshold()
                    {
                      return builder.minTopNThreshold;
                    }
                  };
                }

                @Provides
                public QueryWatcher getQueryWatcher()
                {
                  return QueryRunnerTestHelper.NOOP_QUERYWATCHER;
                }
              }
          ),
          new BuiltInTypesModule(),
          new TestSqlModule(),
          RobuxModule.override(
              new ServerModule(),
              new Module()
              {
                @Provides
                @Self
                @LazySingleton
                public RobuxNode makeSelfRobuxNode()
                {
                  return new RobuxNode("robux/broker", "local-test-host", false, 12345, 443, true, false);
                }

                @Override
                public void configure(Binder binder)
                {
                }
              }
              ),
          new LifecycleModule(),
          RobuxModule.override(
              new QueryableModule(),
              binder -> {
                TestRequestLogger testRequestLogger = new TestRequestLogger();
                binder.bind(RequestLogger.class).toInstance(testRequestLogger);
              }
          ),
          RobuxModule.override(
              new SqlModule(),
              new Module()
              {
                @Override
                public void configure(Binder binder)
                {
                }

                @Provides
                @LazySingleton
                ViewManager createViewManager(Builder builder)
                {
                  return builder.componentSupplier.getPlannerComponentSupplier().createViewManager();
                }

                @Provides
                @LazySingleton
                private RobuxSchema makeRobuxSchema(
                    final Injector injector,
                    QueryRunnerFactoryConglomerate conglomerate,
                    QuerySegmentWalker walker,
                    Builder builder,
                    TimelineServerView timelineServerView
                )
                {
                  return QueryFrameworkUtils.createMockSchema(
                      injector,
                      conglomerate,
                      (SpecificSegmentsQuerySegmentWalker) walker,
                      builder.componentSupplier.getPlannerComponentSupplier().createSchemaManager(),
                      builder.catalogResolver,
                      timelineServerView
                  );
                }

                @Provides
                @LazySingleton
                private SystemSchema makeSystemSchema(
                    AuthorizerMapper authorizerMapper,
                    RobuxSchema robuxSchema,
                    TimelineServerView timelineServerView)
                {
                  return CalciteTests.createMockSystemSchema(robuxSchema, timelineServerView, authorizerMapper);
                }

                @Provides
                @LazySingleton
                private TimelineServerView makeTimelineServerView(SpecificSegmentsQuerySegmentWalker walker)
                {
                  return new TestTimelineServerView(walker.getSegments());
                }

                @Provides
                @LazySingleton
                private LookupSchema makeLookupSchema(final Injector injector)
                {
                  return QueryFrameworkUtils.createMockLookupSchema(injector);
                }

                @Provides
                @LazySingleton
                private RobuxSchemaCatalog makeCatalog(
                    final PlannerConfig plannerConfig,
                    final ViewManager viewManager,
                    AuthorizerMapper authorizerMapper,
                    RobuxSchema robuxSchema,
                    SystemSchema systemSchema,
                    LookupSchema lookupSchema,
                    RobuxOperatorTable createOperatorTable
                )
                {
                  final RobuxSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
                      plannerConfig,
                      viewManager,
                      authorizerMapper,
                      robuxSchema,
                      systemSchema,
                      lookupSchema,
                      createOperatorTable
                  );
                  return rootSchema;
                }
              }
          ),
          new TestSetupModule(),
          new TestSchemaSetupModule(),
          new StorageNodeModule()
      );
    }

    @Override
    public RobuxModule getOverrideModule()
    {
      return RobuxModuleCollection.of();
    }

    /**
     * Configures Guice modules (mostly).
     *
     * Deprecated; see: {@link QueryComponentSupplier#configureGuice(RobuxInjectorBuilder, List)}
     */
    @Deprecated
    protected void configureGuice(RobuxInjectorBuilder builder)
    {
    }

    @Override
    @Deprecated
    public void configureGuice(RobuxInjectorBuilder builder, List<Module> overrideModules)
    {
      configureGuice(builder);
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      return TestDataBuilder.addDataSetsToWalker(tempDirProducer.newTempFolder("segments"), walker);
    }

    @Override
    public Class<? extends SqlEngine> getSqlEngineClass()
    {
      return NativeSqlEngine.class;
    }

    @Override
    public JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider)
    {
      return new JoinableFactoryWrapper(
          QueryStackTests.makeJoinableFactoryFromDefault(
              lookupProvider,
              ImmutableSet.of(TestDataBuilder.CUSTOM_ROW_TABLE_JOINABLE),
              ImmutableMap.of(TestDataBuilder.CUSTOM_ROW_TABLE_JOINABLE.getClass(), GlobalTableDataSource.class)
          )
      );
    }

    @Override
    public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
    {
    }

    @Override
    public PlannerComponentSupplier getPlannerComponentSupplier()
    {
      return plannerComponentSupplier;
    }

    @Override
    public void close() throws IOException
    {
      tempDirProducer.close();
    }

    @Override
    public Boolean isExplainSupported()
    {
      return true;
    }

    @Override
    public QueryRunnerFactoryConglomerate wrapConglomerate(
        QueryRunnerFactoryConglomerate conglomerate,
        Closer resourceCloser
    )
    {
      return conglomerate;
    }

    @Override
    public final TempDirProducer getTempDirProducer()
    {
      return tempDirProducer;
    }
  }

  public static class StandardPlannerComponentSupplier implements PlannerComponentSupplier
  {
    @Override
    public Set<ExtensionCalciteRuleProvider> extensionCalciteRules()
    {
      return ImmutableSet.of();
    }

    @Override
    public ViewManager createViewManager()
    {
      return new InProcessViewManager(ROBUX_VIEW_MACRO_FACTORY);
    }

    @Override
    public void populateViews(ViewManager viewManager, PlannerFactory plannerFactory)
    {
      viewManager.createView(
          plannerFactory,
          "aview",
          "SELECT SUBSTRING(dim1, 1, 1) AS dim1_firstchar FROM foo WHERE dim2 = 'a'"
      );

      viewManager.createView(
          plannerFactory,
          "bview",
          "SELECT COUNT(*) FROM robux.foo\n"
          + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'"
      );

      viewManager.createView(
          plannerFactory,
          "cview",
          "SELECT SUBSTRING(bar.dim1, 1, 1) AS dim1_firstchar, bar.dim2 as dim2, dnf.l2 as l2\n"
          + "FROM (SELECT * from foo WHERE dim2 = 'a') as bar INNER JOIN robux.numfoo dnf ON bar.dim2 = dnf.dim2"
      );

      viewManager.createView(
          plannerFactory,
          "dview",
          "SELECT SUBSTRING(dim1, 1, 1) AS numfoo FROM foo WHERE dim2 = 'a'"
      );

      viewManager.createView(
          plannerFactory,
          "forbiddenView",
          "SELECT __time, SUBSTRING(dim1, 1, 1) AS dim1_firstchar, dim2 FROM foo WHERE dim2 = 'a'"
      );

      viewManager.createView(
          plannerFactory,
          "restrictedView",
          "SELECT __time, dim1, dim2, m1 FROM robux.forbiddenDatasource WHERE dim2 = 'a'"
      );

      viewManager.createView(
          plannerFactory,
          "invalidView",
          "SELECT __time, dim1, dim2, m1 FROM robux.invalidDatasource WHERE dim2 = 'a'"
      );
    }

    @Override
    public RobuxSchemaManager createSchemaManager()
    {
      return new NoopRobuxSchemaManager();
    }

    @Override
    public void finalizePlanner(PlannerFixture plannerFixture)
    {
    }
  }

  /**
   * Builder for the framework. The component supplier and injector are
   * required; all other items are optional.
   */
  public static class Builder
  {
    private final QueryComponentSupplier componentSupplier;
    private int minTopNThreshold = TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;
    private int mergeBufferCount;
    private CatalogResolver catalogResolver = CatalogResolver.NULL_RESOLVER;
    private List<Module> overrideModules = new ArrayList<>();
    private SqlTestFrameworkConfig config;
    private Closer resourceCloser = Closer.create();

    public Builder(QueryComponentSupplier componentSupplier)
    {
      this.componentSupplier = componentSupplier;
    }

    public Builder minTopNThreshold(int minTopNThreshold)
    {
      this.minTopNThreshold = minTopNThreshold;
      return this;
    }

    public Builder mergeBufferCount(int mergeBufferCount)
    {
      this.mergeBufferCount = mergeBufferCount;
      return this;
    }

    public Builder catalogResolver(CatalogResolver catalogResolver)
    {
      this.catalogResolver = catalogResolver;
      return this;
    }

    public Builder withOverrideModule(Module m)
    {
      this.overrideModules.add(m);
      return this;
    }

    public SqlTestFramework build()
    {
      return new SqlTestFramework(this);
    }

    public Builder withConfig(SqlTestFrameworkConfig config)
    {
      this.config = config;
      return this;
    }

    public QueryComponentSupplier getComponentSupplier()
    {
      return componentSupplier;
    }

    public CatalogResolver getCatalogResolver()
    {
      return catalogResolver;
    }

    public Closer getResourceCloser()
    {
      return resourceCloser;
    }
  }

  /**
   * Builds the statement factory, which also builds all the infrastructure
   * behind the factory by calling methods on this test class. As a result, each
   * factory is specific to one test and one planner config. This method can be
   * overridden to control the objects passed to the factory.
   */
  public static class PlannerFixture
  {
    private final ViewManager viewManager;
    private final PlannerFactory plannerFactory;
    private final SqlStatementFactory statementFactory;

    public PlannerFixture(
        final SqlTestFramework framework,
        final PlannerComponentSupplier componentSupplier,
        final PlannerConfig plannerConfig,
        final AuthConfig authConfig
    )
    {
      this.viewManager = componentSupplier.createViewManager();
      final RobuxSchemaCatalog rootSchema = QueryFrameworkUtils.createMockRootSchema(
          framework.injector,
          framework.conglomerate(),
          framework.walker(),
          plannerConfig,
          viewManager,
          componentSupplier.createSchemaManager(),
          framework.authorizerMapper,
          framework.builder.catalogResolver,
          framework.injector.getInstance(TimelineServerView.class)
      );

      this.plannerFactory = new PlannerFactory(
          rootSchema,
          framework.operatorTable(),
          framework.macroTable(),
          plannerConfig,
          framework.authorizerMapper,
          framework.queryJsonMapper(),
          CalciteTests.ROBUX_SCHEMA_NAME,
          new CalciteRulesManager(componentSupplier.extensionCalciteRules()),
          framework.injector.getInstance(JoinableFactoryWrapper.class),
          framework.builder.catalogResolver,
          authConfig != null ? authConfig : new AuthConfig(),
          NoopPolicyEnforcer.instance(),
          new RobuxHookDispatcher()
      );
      componentSupplier.finalizePlanner(this);
      this.statementFactory = QueryFrameworkUtils.createSqlMultiStatementFactory(framework.engine, plannerFactory);
      componentSupplier.populateViews(viewManager, plannerFactory);
    }

    public ViewManager viewManager()
    {
      return viewManager;
    }

    public PlannerFactory plannerFactory()
    {
      return plannerFactory;
    }

    public SqlStatementFactory statementFactory()
    {
      return statementFactory;
    }
  }

  /**
   * Guice module to create the various query framework items. By creating items within
   * a module, later items can depend on those created earlier by grabbing them from the
   * injector. This avoids the race condition that otherwise occurs if we try to build
   * some of the items directly code, while others depend on the injector.
   * <p>
   * To allow customization, the instances are created via provider methods that pull
   * dependencies from Guice, then call the component provider to create the instance.
   * Tests customize the instances by overriding the instance creation methods.
   * <p>
   * This is an intermediate solution: the ultimate solution is to create things
   * in Guice itself.
   */
  public static class TestSetupModule implements RobuxModule
  {
    @Provides
    TempDirProducer getTempDirProducer(Builder builder)
    {
      return builder.componentSupplier.getTempDirProducer();
    }

    @Provides
    ServiceEmitter getServiceEmitter()
    {
      return NoopServiceEmitter.instance();
    }

    @Provides
    @LazySingleton
    public QuerySegmentWalker getQuerySegmentWalker(SpecificSegmentsQuerySegmentWalker walker)
    {
      return walker;
    }

    @Provides
    ChatHandlerProvider getChatHandlerProvider()
    {
      return new NoopChatHandlerProvider();
    }

    @Override
    public void configure(Binder binder)
    {
      binder.bind(RobuxOperatorTable.class).in(LazySingleton.class);
      binder.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
    }

    @Provides
    @Global
    NonBlockingPool<ByteBuffer> getGlobalPool(TestBufferPool pool)
    {
      return pool;
    }

    @Provides
    @Merging
    BlockingPool<ByteBuffer> getMergingPool(TestBufferPool pool)
    {
      return pool;
    }

    @Provides
    AuthorizerMapper getAuthorizerMapper()
    {
      return AuthTestUtils.TEST_AUTHORIZER_MAPPER;
    }

    @Provides
    @LazySingleton
    private GroupByResourcesReservationPool makeGroupByResourcesReservationPool(
        final GroupByQueryConfig config,
        final TestGroupByBuffers bufferPools
    )
    {
      return new GroupByResourcesReservationPool(bufferPools.getMergePool(), config);
    }

    @Provides
    @LazySingleton
    private GroupingEngine makeGroupingEngine(
        final ObjectMapper mapper,
        final RobuxProcessingConfig processingConfig,
        final GroupByStatsProvider statsProvider,
        final GroupByQueryConfig config,
        final GroupByResourcesReservationPool groupByResourcesReservationPool
    )
    {
      final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
      return new GroupingEngine(
          processingConfig,
          configSupplier,
          groupByResourcesReservationPool,
          mapper,
          mapper,
          QueryRunnerTestHelper.NOOP_QUERYWATCHER,
          statsProvider
      );
    }

    @Provides
    @LazySingleton
    @Merging
    GroupByResourcesReservationPool makeMergingGroupByResourcesReservationPool(
        final GroupByResourcesReservationPool groupByResourcesReservationPool
    )
    {
      return groupByResourcesReservationPool;
    }

    @Provides
    @LazySingleton
    public RobuxProcessingConfig makeProcessingConfig(Builder builder)
    {
      return QueryStackTests.getProcessingConfig(builder.mergeBufferCount);
    }

    @Provides
    @LazySingleton
    public TestBufferPool makeTestBufferPool(Builder builder)
    {
      return QueryStackTests.makeTestBufferPool(builder.resourceCloser);
    }

    @Provides
    @LazySingleton
    public TestGroupByBuffers makeTestGroupByBuffers(RobuxProcessingConfig processingConfig, Builder builder)
    {
      return QueryStackTests.makeGroupByBuffers(builder.resourceCloser, processingConfig);
    }

    @Provides
    @LazySingleton
    public JoinableFactoryWrapper joinableFactoryWrapper(final Injector injector, Builder builder)
    {
      return builder.componentSupplier.createJoinableFactoryWrapper(
          injector.getInstance(LookupExtractorFactoryContainerProvider.class)
      );
    }

    @Provides
    @LazySingleton
    public QueryLifecycleFactory queryLifecycleFactory(final Injector injector)
    {
      return QueryFrameworkUtils.createMockQueryLifecycleFactory(
          injector.getInstance(QuerySegmentWalker.class),
          injector.getInstance(QueryRunnerFactoryConglomerate.class),
          injector.getInstance(AuthorizerMapper.class)
      );
    }

    @Provides
    SqlTestFrameworkConfig getTestConfig(Builder builder)
    {
      return builder.config;
    }

    @Provides
    @Named("quidem")
    public URI getRobuxTestURI(SqlTestFrameworkConfig config)
    {
      return config.getRobuxTestURI();
    }
  }

  public static class TestSchemaSetupModule implements RobuxModule
  {
    @Provides
    @LazySingleton
    public SpecificSegmentsQuerySegmentWalker specificSegmentsQuerySegmentWalker(
        @Named("empty") SpecificSegmentsQuerySegmentWalker walker, Builder builder,
        List<TestDataSet> testDataSets)
    {
      builder.resourceCloser.register(walker);
      if (testDataSets.isEmpty()) {
        builder.componentSupplier.addSegmentsToWalker(walker);
      } else {
        for (TestDataSet testDataSet : testDataSets) {
          walker.add(testDataSet, builder.componentSupplier.getTempDirProducer().newTempFolder());
        }
      }

      return walker;
    }

    @Provides
    @LazySingleton
    public List<TestDataSet> buildCustomTables(ObjectMapper objectMapper, TempDirProducer tdp,
        SqlTestFrameworkConfig cfg)
    {
      String datasets = cfg.datasets;
      if (datasets.isEmpty()) {
        return Collections.emptyList();
      }
      final File[] inputFiles = getTableIngestFiles(datasets);
      List<TestDataSet> ret = new ArrayList<TestDataSet>();
      for (File src : inputFiles) {
        ret.add(FakeIndexTaskUtil.makeDS(objectMapper, src));
      }
      return ret;
    }

    private File[] getTableIngestFiles(String datasets)
    {
      File datasetsFile = ProjectPathUtils.getPathFromProjectRoot(datasets);
      if (!datasetsFile.exists()) {
        throw new RE("Table config file does not exist: %s", datasetsFile);
      }
      if (!datasetsFile.isDirectory()) {
        throw new RE("The option datasets [%s] must point to a directory relative to the project root!", datasetsFile);
      }
      final File[] inputFiles = datasetsFile.listFiles(this::jsonFiles);
      if (inputFiles.length == 0) {
        throw new RE("There are no json files found in datasets directory [%s]!", datasetsFile);
      }

      return inputFiles;
    }

    boolean jsonFiles(File f)
    {
      return !f.isDirectory() && f.getName().endsWith(".json");
    }

    @Provides
    @LazySingleton
    public TestSegmentsBroker makeTimelines()
    {
      return new TestSegmentsBroker();
    }

    @Provides
    @LazySingleton
    private HttpClient makeHttpClient(ObjectMapper objectMapper)
    {
      return new TestHttpClient(objectMapper);
    }

    @Provides
    @Named("empty")
    @LazySingleton
    public SpecificSegmentsQuerySegmentWalker createEmptyWalker(
        TestSegmentsBroker testSegmentsBroker,
        ClientQuerySegmentWalker clientQuerySegmentWalker)
    {
      return new SpecificSegmentsQuerySegmentWalker(
          testSegmentsBroker.timelines,
          clientQuerySegmentWalker
      );
    }

    @Provides
    @LazySingleton
    private ClientQuerySegmentWalker makeClientQuerySegmentWalker(QueryRunnerFactoryConglomerate conglomerate,
        JoinableFactoryWrapper joinableFactory, Injector injector, ServiceEmitter emitter,
        TestClusterQuerySegmentWalker testClusterQuerySegmentWalker,
        LocalQuerySegmentWalker testLocalQuerySegmentWalker, ServerConfig serverConfig)
    {
      return new ClientQuerySegmentWalker(
          emitter,
          testClusterQuerySegmentWalker,
          testLocalQuerySegmentWalker,
          conglomerate,
          joinableFactory.getJoinableFactory(),
          new RetryQueryRunnerConfig(),
          injector.getInstance(ObjectMapper.class),
          serverConfig,
          injector.getInstance(Cache.class),
          injector.getInstance(CacheConfig.class),
          new SubqueryGuardrailHelper(null, JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(), 1),
          new SubqueryCountStatsProvider(),
          new DefaultGenericQueryMetricsFactory()
      );
    }

    @Provides
    @LazySingleton
    public SubqueryGuardrailHelper makeSubqueryGuardrailHelper()
    {
      return new SubqueryGuardrailHelper(null, JvmUtils.getRuntimeInfo().getMaxHeapSizeBytes(), 1);
    }

    @Override
    public void configure(Binder binder)
    {
    }
  }

  public static final RobuxViewMacroFactory ROBUX_VIEW_MACRO_FACTORY = new TestRobuxViewMacroFactory();

  private final Builder builder;
  private final QueryComponentSupplier componentSupplier;
  private final Injector injector;
  private final AuthorizerMapper authorizerMapper = CalciteTests.TEST_AUTHORIZER_MAPPER;
  private final SqlEngine engine;

  private SqlTestFramework(Builder builder)
  {
    this.builder = builder;
    this.componentSupplier = builder.componentSupplier;
    Properties properties = new Properties();
    this.componentSupplier.gatherProperties(properties);
    Injector startupInjector = new StartupInjectorBuilder()
        .withProperties(properties)
        .build();
    CoreInjectorBuilder injectorBuilder = (CoreInjectorBuilder) new CoreInjectorBuilder(startupInjector)
        // Ignore load scopes. This is a unit test, not a Robux node. If a
        // test pulls in a module, then pull in that module, even though we are
        // not the Robux node to which the module is scoped.
        .ignoreLoadScopes();

    injectorBuilder.addAll(RobuxModuleCollection.flatten(componentSupplier.getCoreModule()));
    injectorBuilder.addModule(binder -> binder.bind(Builder.class).toInstance(builder));

    ArrayList<Module> overrideModules = new ArrayList<>(builder.overrideModules);
    overrideModules.addAll(RobuxModuleCollection.flatten(componentSupplier.getOverrideModule()));
    builder.componentSupplier.configureGuice(injectorBuilder, overrideModules);

    ServiceInjectorBuilder serviceInjector = new ServiceInjectorBuilder(injectorBuilder);
    serviceInjector.addAll(overrideModules);

    this.injector = serviceInjector.build();
    this.engine = injector.getInstance(componentSupplier.getSqlEngineClass());

    componentSupplier.configureJsonMapper(queryJsonMapper());
    componentSupplier.finalizeTestFramework(this);
  }

  public Injector injector()
  {
    return injector;
  }

  public SqlEngine engine()
  {
    return engine;
  }

  public ObjectMapper queryJsonMapper()
  {
    return injector.getInstance(ObjectMapper.class);
  }

  public QueryLifecycleFactory queryLifecycleFactory()
  {
    return injector.getInstance(QueryLifecycleFactory.class);
  }

  public QueryLifecycle queryLifecycle()
  {
    return queryLifecycleFactory().factorize();
  }

  public ExprMacroTable macroTable()
  {
    return injector.getInstance(ExprMacroTable.class);
  }

  public RobuxOperatorTable operatorTable()
  {
    return injector.getInstance(RobuxOperatorTable.class);
  }

  public SpecificSegmentsQuerySegmentWalker walker()
  {
    return injector.getInstance(SpecificSegmentsQuerySegmentWalker.class);
  }

  public QueryRunnerFactoryConglomerate conglomerate()
  {
    return injector.getInstance(QueryRunnerFactoryConglomerate.class);
  }

  /**
   * Creates an object (a "fixture") to hold the planner factory, view manager
   * and related items. Most tests need just the statement factory. View-related
   * tests also use the view manager. The fixture builds the infrastructure
   * behind the factory by calling methods on the {@link QueryComponentSupplier}
   * interface. That Calcite tests that interface, so the components can be customized
   * by overriding methods in a particular tests. As a result, each
   * planner fixture is specific to one test and one planner config.
   */
  public PlannerFixture plannerFixture(
      PlannerConfig plannerConfig,
      AuthConfig authConfig
  )
  {
    PlannerComponentSupplier plannerComponentSupplier = componentSupplier.getPlannerComponentSupplier();
    return new PlannerFixture(this, plannerComponentSupplier, plannerConfig, authConfig);
  }

  public void close()
  {
    try {
      builder.resourceCloser.close();
      componentSupplier.close();
    }
    catch (IOException e) {
      throw new RE(e);
    }
  }

  public URI getRobuxTestURI()
  {
    return builder.config.getRobuxTestURI();
  }
}

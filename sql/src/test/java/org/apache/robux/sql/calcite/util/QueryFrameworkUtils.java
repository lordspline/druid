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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.robux.client.InternalQueryConfig;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.query.DefaultGenericQueryMetricsFactory;
import org.apache.robux.query.DefaultQueryConfig;
import org.apache.robux.query.GlobalTableDataSource;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.segment.join.JoinableFactory;
import org.apache.robux.segment.loading.SegmentCacheManager;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.server.QueryLifecycleFactory;
import org.apache.robux.server.QueryStackTests;
import org.apache.robux.server.SegmentManager;
import org.apache.robux.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.robux.server.log.NoopRequestLogger;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.server.security.AuthConfig;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.sql.DirectStatement;
import org.apache.robux.sql.PreparedStatement;
import org.apache.robux.sql.SqlLifecycleManager;
import org.apache.robux.sql.SqlQueryPlus;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.SqlToolbox;
import org.apache.robux.sql.calcite.planner.CatalogResolver;
import org.apache.robux.sql.calcite.planner.RobuxOperatorTable;
import org.apache.robux.sql.calcite.planner.RobuxPlanner;
import org.apache.robux.sql.calcite.planner.PlannerConfig;
import org.apache.robux.sql.calcite.planner.PlannerFactory;
import org.apache.robux.sql.calcite.run.SqlEngine;
import org.apache.robux.sql.calcite.schema.BrokerSegmentMetadataCache;
import org.apache.robux.sql.calcite.schema.BrokerSegmentMetadataCacheConfig;
import org.apache.robux.sql.calcite.schema.RobuxSchema;
import org.apache.robux.sql.calcite.schema.RobuxSchemaCatalog;
import org.apache.robux.sql.calcite.schema.RobuxSchemaManager;
import org.apache.robux.sql.calcite.schema.InformationSchema;
import org.apache.robux.sql.calcite.schema.LookupSchema;
import org.apache.robux.sql.calcite.schema.NamedRobuxSchema;
import org.apache.robux.sql.calcite.schema.NamedLookupSchema;
import org.apache.robux.sql.calcite.schema.NamedSchema;
import org.apache.robux.sql.calcite.schema.NamedSystemSchema;
import org.apache.robux.sql.calcite.schema.NamedViewSchema;
import org.apache.robux.sql.calcite.schema.NoopRobuxSchemaManager;
import org.apache.robux.sql.calcite.schema.PhysicalDatasourceMetadataFactory;
import org.apache.robux.sql.calcite.schema.SystemSchema;
import org.apache.robux.sql.calcite.schema.ViewSchema;
import org.apache.robux.sql.calcite.view.ViewManager;
import org.easymock.EasyMock;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryFrameworkUtils
{
  public static final String INFORMATION_SCHEMA_NAME = "INFORMATION_SCHEMA";

  public static QueryLifecycleFactory createMockQueryLifecycleFactory(
      final QuerySegmentWalker walker,
      final QueryRunnerFactoryConglomerate conglomerate,
      final AuthorizerMapper authorizerMapper
  )
  {
    return new QueryLifecycleFactory(
        conglomerate,
        walker,
        new DefaultGenericQueryMetricsFactory(),
        NoopServiceEmitter.instance(),
        NoopRequestLogger.instance(),
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        authorizerMapper,
        Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of()))
    );
  }

  /**
   * Create a standard {@link SqlStatementFactory} for testing with a new {@link SqlToolbox} created by
   * {@link #createTestToolbox(SqlEngine, PlannerFactory)}
   */
  public static SqlStatementFactory createSqlStatementFactory(
      final SqlEngine engine,
      final PlannerFactory plannerFactory
  )
  {
    return new SqlStatementFactory(createTestToolbox(engine, plannerFactory));
  }

  /**
   * Create a {@link TestMultiStatementFactory}, a special {@link SqlStatementFactory} which allows multi-statement SET
   * parsing for {@link SqlStatementFactory#directStatement(SqlQueryPlus)} and
   * {@link SqlStatementFactory#preparedStatement(SqlQueryPlus)}.
   */
  public static SqlStatementFactory createSqlMultiStatementFactory(
      final SqlEngine engine,
      final PlannerFactory plannerFactory
  )
  {
    return new TestMultiStatementFactory(createTestToolbox(engine, plannerFactory), engine, plannerFactory);
  }

  private static SqlToolbox createTestToolbox(SqlEngine engine, PlannerFactory plannerFactory)
  {
    return new SqlToolbox(
        engine,
        plannerFactory,
        NoopServiceEmitter.instance(),
        NoopRequestLogger.instance(),
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new DefaultQueryConfig(ImmutableMap.of()),
        new SqlLifecycleManager()
    );
  }

  public static RobuxSchemaCatalog createMockRootSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      @Nullable final ViewManager viewManager,
      final RobuxSchemaManager robuxSchemaManager,
      final AuthorizerMapper authorizerMapper,
      final CatalogResolver catalogResolver)
  {
    TimelineServerView timelineServerView = new TestTimelineServerView(walker.getSegments());
    return createMockRootSchema(
        injector,
        conglomerate,
        walker,
        plannerConfig,
        viewManager,
        robuxSchemaManager,
        authorizerMapper,
        catalogResolver,
        timelineServerView
    );
  }

  public static RobuxSchemaCatalog createMockRootSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      @Nullable final ViewManager viewManager,
      final RobuxSchemaManager robuxSchemaManager,
      final AuthorizerMapper authorizerMapper,
      final CatalogResolver catalogResolver,
      final TimelineServerView timelineServerView
  )
  {
    RobuxSchema robuxSchema = createMockSchema(
        injector,
        conglomerate,
        walker,
        robuxSchemaManager,
        catalogResolver,
        timelineServerView
    );
    SystemSchema systemSchema =
        CalciteTests.createMockSystemSchema(robuxSchema, timelineServerView, authorizerMapper);

    LookupSchema lookupSchema = createMockLookupSchema(injector);
    RobuxOperatorTable createOperatorTable = createOperatorTable(injector);

    return createMockRootSchema(
        plannerConfig,
        viewManager,
        authorizerMapper,
        robuxSchema,
        systemSchema,
        lookupSchema,
        createOperatorTable
    );
  }

  public static RobuxSchemaCatalog createMockRootSchema(
      final PlannerConfig plannerConfig,
      final ViewManager viewManager,
      final AuthorizerMapper authorizerMapper,
      RobuxSchema robuxSchema,
      SystemSchema systemSchema,
      LookupSchema lookupSchema,
      RobuxOperatorTable createOperatorTable
  )
  {
    ViewSchema viewSchema = viewManager != null ? new ViewSchema(viewManager) : null;

    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();
    Set<NamedSchema> namedSchemas = new HashSet<>();
    namedSchemas.add(new NamedRobuxSchema(robuxSchema, CalciteTests.ROBUX_SCHEMA_NAME));
    namedSchemas.add(new NamedSystemSchema(plannerConfig, systemSchema));
    namedSchemas.add(new NamedLookupSchema(lookupSchema));

    if (viewSchema != null) {
      namedSchemas.add(new NamedViewSchema(viewSchema));
    }

    RobuxSchemaCatalog catalog = new RobuxSchemaCatalog(
        rootSchema,
        namedSchemas.stream().collect(Collectors.toMap(NamedSchema::getSchemaName, x -> x))
    );
    InformationSchema informationSchema =
        new InformationSchema(
            catalog,
            authorizerMapper,
            createOperatorTable
        );
    rootSchema.add(CalciteTests.ROBUX_SCHEMA_NAME, robuxSchema);
    rootSchema.add(INFORMATION_SCHEMA_NAME, informationSchema);
    rootSchema.add(NamedSystemSchema.NAME, systemSchema);
    rootSchema.add(NamedLookupSchema.NAME, lookupSchema);

    if (viewSchema != null) {
      rootSchema.add(NamedViewSchema.NAME, viewSchema);
    }

    return catalog;
  }

  public static RobuxSchemaCatalog createMockRootSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper
  )
  {
    return createMockRootSchema(
        injector,
        conglomerate,
        walker,
        plannerConfig,
        null,
        new NoopRobuxSchemaManager(),
        authorizerMapper,
        CatalogResolver.NULL_RESOLVER
    );
  }

  public static RobuxSchema createMockSchema(
      final Injector injector,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final RobuxSchemaManager robuxSchemaManager,
      final CatalogResolver catalog,
      final TimelineServerView timelineServerView
  )
  {
    final BrokerSegmentMetadataCache cache = new BrokerSegmentMetadataCache(
        createMockQueryLifecycleFactory(walker, conglomerate, CalciteTests.TEST_AUTHORIZER_MAPPER),
        timelineServerView,
        BrokerSegmentMetadataCacheConfig.create(),
        CalciteTests.TEST_AUTHENTICATOR_ESCALATOR,
        new InternalQueryConfig(),
        new NoopServiceEmitter(),
        new PhysicalDatasourceMetadataFactory(
            createDefaultJoinableFactory(injector),
            new SegmentManager(EasyMock.createMock(SegmentCacheManager.class))
            {
              @Override
              public Set<String> getDataSourceNames()
              {
                return ImmutableSet.of(CalciteTests.BROADCAST_DATASOURCE, CalciteTests.RESTRICTED_BROADCAST_DATASOURCE);
              }
            }
        ),
        null,
        CentralizedDatasourceSchemaConfig.create()
    );

    try {
      cache.start();
      cache.awaitInitialization();
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    cache.stop();
    return new RobuxSchema(cache, robuxSchemaManager, catalog);
  }

  public static JoinableFactory createDefaultJoinableFactory(Injector injector)
  {
    return QueryStackTests.makeJoinableFactoryFromDefault(
        injector.getInstance(LookupExtractorFactoryContainerProvider.class),
        ImmutableSet.of(TestDataBuilder.CUSTOM_ROW_TABLE_JOINABLE),
        ImmutableMap.of(TestDataBuilder.CUSTOM_ROW_TABLE_JOINABLE.getClass(), GlobalTableDataSource.class)
    );
  }

  public static RobuxOperatorTable createOperatorTable(final Injector injector)
  {
    try {
      return injector.getInstance(RobuxOperatorTable.class);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static LookupSchema createMockLookupSchema(final Injector injector)
  {
    return new LookupSchema(injector.getInstance(LookupExtractorFactoryContainerProvider.class));
  }



  /**
   * SqlStatementFactory which overrides direct statement creation to allow calcite tests to test multi-part set
   * statements e.g. like 'SET vectorize = 'force'; SET useApproxCountDistinct = true; SELECT 1 + 1'
   */
  static class TestMultiStatementFactory extends SqlStatementFactory
  {
    private final SqlToolbox toolbox;
    private final SqlEngine engine;
    private final PlannerFactory plannerFactory;

    public TestMultiStatementFactory(SqlToolbox lifecycleToolbox, SqlEngine engine, PlannerFactory plannerFactory)
    {
      super(lifecycleToolbox);
      this.toolbox = lifecycleToolbox;
      this.engine = engine;
      this.plannerFactory = plannerFactory;
    }

    @Override
    public DirectStatement directStatement(SqlQueryPlus sqlRequest)
    {
      // override direct statement creation to allow calcite tests to test multi-part set statements
      return new DirectStatement(toolbox, sqlRequest)
      {
        @Override
        protected RobuxPlanner createPlanner()
        {
          return plannerFactory.createPlanner(
              engine,
              queryPlus.sql(),
              queryPlus.sqlNode(),
              queryContext,
              hook
          );
        }
      };
    }

    @Override
    public PreparedStatement preparedStatement(SqlQueryPlus sqlRequest)
    {
      return new PreparedStatement(toolbox, sqlRequest)
      {
        @Override
        protected RobuxPlanner getPlanner()
        {
          return plannerFactory.createPlanner(
              engine,
              queryPlus.sql(),
              queryPlus.sqlNode(),
              queryContext,
              hook
          );
        }

        @Override
        public DirectStatement execute(List<TypedValue> parameters)
        {
          return directStatement(queryPlus.withParameters(parameters));
        }
      };
    }
  }
}

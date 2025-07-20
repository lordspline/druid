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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import org.apache.robux.catalog.model.TableDefnRegistry;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.PolyBind;
import org.apache.robux.guice.annotations.NativeQuery;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.query.DefaultQueryConfig;
import org.apache.robux.server.QueryScheduler;
import org.apache.robux.server.log.RequestLogger;
import org.apache.robux.sql.SqlLifecycleManager;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.SqlToolbox;
import org.apache.robux.sql.avatica.AvaticaModule;
import org.apache.robux.sql.calcite.aggregation.SqlAggregationModule;
import org.apache.robux.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.robux.sql.calcite.planner.CalcitePlannerModule;
import org.apache.robux.sql.calcite.planner.CatalogResolver;
import org.apache.robux.sql.calcite.planner.PlannerFactory;
import org.apache.robux.sql.calcite.run.NativeSqlEngine;
import org.apache.robux.sql.calcite.run.SqlEngine;
import org.apache.robux.sql.calcite.schema.RobuxCalciteSchemaModule;
import org.apache.robux.sql.calcite.schema.RobuxSchemaManager;
import org.apache.robux.sql.calcite.schema.NoopRobuxSchemaManager;
import org.apache.robux.sql.calcite.view.RobuxViewModule;
import org.apache.robux.sql.calcite.view.NoopViewManager;
import org.apache.robux.sql.calcite.view.ViewManager;
import org.apache.robux.sql.http.SqlHttpModule;

import java.util.Properties;

public class SqlModule implements Module
{
  public static final String PROPERTY_SQL_ENABLE = "robux.sql.enable";
  public static final String PROPERTY_SQL_ENABLE_JSON_OVER_HTTP = "robux.sql.http.enable";
  public static final String PROPERTY_SQL_ENABLE_AVATICA = "robux.sql.avatica.enable";
  public static final String PROPERTY_SQL_VIEW_MANAGER_TYPE = "robux.sql.viewmanager.type";
  public static final String PROPERTY_SQL_SCHEMA_MANAGER_TYPE = "robux.sql.schemamanager.type";
  public static final String PROPERTY_SQL_APPROX_COUNT_DISTINCT_CHOICE = "robux.sql.approxCountDistinct.function";

  private Properties props;

  @Inject
  public void setProps(Properties props)
  {
    this.props = props;
  }

  @Override
  public void configure(Binder binder)
  {
    if (!isEnabled()) {
      return;
    }

    PolyBind.optionBinder(binder, Key.get(ViewManager.class))
            .addBinding(NoopViewManager.TYPE)
            .to(NoopViewManager.class)
            .in(LazySingleton.class);

    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY_SQL_VIEW_MANAGER_TYPE,
        Key.get(ViewManager.class),
        NoopViewManager.TYPE
    );

    PolyBind.optionBinder(binder, Key.get(RobuxSchemaManager.class))
            .addBinding(NoopRobuxSchemaManager.TYPE)
            .to(NoopRobuxSchemaManager.class)
            .in(LazySingleton.class);

    PolyBind.createChoiceWithDefault(
        binder,
        PROPERTY_SQL_SCHEMA_MANAGER_TYPE,
        Key.get(RobuxSchemaManager.class),
        NoopRobuxSchemaManager.TYPE
    );

    binder.bind(TableDefnRegistry.class).in(LazySingleton.class);

    binder.install(new RobuxCalciteSchemaModule());
    binder.install(new CalcitePlannerModule());
    binder.install(new SqlAggregationModule());
    binder.install(new RobuxViewModule());

    binder.install(new SqlStatementFactoryModule());

    // QueryLookupOperatorConversion isn't in RobuxOperatorTable since it needs a LookupExtractorFactoryContainerProvider injected.
    SqlBindings.addOperatorConversion(binder, QueryLookupOperatorConversion.class);

    if (isJsonOverHttpEnabled()) {
      binder.install(new SqlHttpModule());
    }

    if (isAvaticaEnabled()) {
      binder.install(new AvaticaModule());
    }

    // Default do-nothing catalog resolver
    binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);

    // Bind the engine
    Multibinder.newSetBinder(binder, SqlEngine.class)
               .addBinding()
               .to(NativeSqlEngine.class)
               .in(LazySingleton.class);
  }

  private boolean isEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE, "true"));
  }

  private boolean isJsonOverHttpEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true"));
  }

  private boolean isAvaticaEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE_AVATICA, "true"));
  }

  /**
   * We create a new class for this module so that it can be shared by tests.  The structuring of the SqlModule
   * at time of writing was not conducive to reuse in test code, so, instead of fixing that we just take the easy
   * way out of adding the test-reusable code to this module and reuse that.
   *
   * Generally speaking, the injection pattern done by this module is a bit circuitous.  The `SqlToolbox` acts as
   * if it can be injected with all of its dependencies, but also expects to be mutated with a new SqlEngine.  We
   * should likely look at adjusting the object dependencies to actually depend on the SqlToolbox and create
   * different Toolboxes for the different way that queries are done.  But, for now, I'm not changing the interfaces.
   */
  public static class SqlStatementFactoryModule implements Module
  {

    @Provides
    @LazySingleton
    public SqlToolbox makeSqlToolbox(
        final PlannerFactory plannerFactory,
        final ServiceEmitter emitter,
        final RequestLogger requestLogger,
        final QueryScheduler queryScheduler,
        final Supplier<DefaultQueryConfig> defaultQueryConfig,
        final SqlLifecycleManager sqlLifecycleManager
    )
    {
      return new SqlToolbox(
          null,
          plannerFactory,
          emitter,
          requestLogger,
          queryScheduler,
          defaultQueryConfig.get(),
          sqlLifecycleManager
      );
    }

    @Provides
    @NativeQuery
    @LazySingleton
    public SqlStatementFactory makeNativeSqlStatementFactory(
        final NativeSqlEngine sqlEngine,
        SqlToolbox toolbox
    )
    {
      return new SqlStatementFactory(toolbox.withEngine(sqlEngine));
    }

    @Override
    public void configure(Binder binder)
    {
      // Do nothing, this class exists for the Provider methods
    }
  }
}

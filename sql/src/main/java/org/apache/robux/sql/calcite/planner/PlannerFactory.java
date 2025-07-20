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

package org.apache.robux.sql.calcite.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.volcano.RobuxVolcanoCost;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.QueryContexts;
import org.apache.robux.query.policy.PolicyEnforcer;
import org.apache.robux.segment.join.JoinableFactoryWrapper;
import org.apache.robux.server.security.AuthConfig;
import org.apache.robux.server.security.AuthorizationResult;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.server.security.NoopEscalator;
import org.apache.robux.sql.calcite.parser.RobuxSqlParser;
import org.apache.robux.sql.calcite.parser.StatementAndSetContext;
import org.apache.robux.sql.calcite.planner.convertlet.RobuxConvertletTable;
import org.apache.robux.sql.calcite.run.SqlEngine;
import org.apache.robux.sql.calcite.schema.RobuxSchemaCatalog;
import org.apache.robux.sql.calcite.schema.RobuxSchemaName;
import org.apache.robux.sql.hook.RobuxHook;
import org.apache.robux.sql.hook.RobuxHookDispatcher;

import java.util.Map;
import java.util.Properties;

public class PlannerFactory extends PlannerToolbox
{
  @Inject
  public PlannerFactory(
      final RobuxSchemaCatalog rootSchema,
      final RobuxOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper,
      final @Json ObjectMapper jsonMapper,
      final @RobuxSchemaName String robuxSchemaName,
      final CalciteRulesManager calciteRuleManager,
      final JoinableFactoryWrapper joinableFactoryWrapper,
      final CatalogResolver catalog,
      final AuthConfig authConfig,
      final PolicyEnforcer policyEnforcer,
      final RobuxHookDispatcher hookDispatcher
  )
  {
    super(
        operatorTable,
        macroTable,
        jsonMapper,
        plannerConfig,
        rootSchema,
        joinableFactoryWrapper,
        catalog,
        robuxSchemaName,
        calciteRuleManager,
        authorizerMapper,
        authConfig,
        policyEnforcer,
        hookDispatcher
    );
  }

  /**
   * Create a Robux query planner from an initial query context. If allowSetStatementsToBuildContext is set to true,
   * the parser is allowed to parse multi-part SQL statements where all statements in the list except the last one are
   * SET statements, for example 'SET x = 'y'; SET foo = 123; SELECT ...', where these values will be added to the
   * {@link org.apache.robux.query.QueryContext} of the final statement.
   *
   * @param engine       current SQL engine
   * @param sql          sql query string
   * @param sqlNode      parsed sql query, from {@link RobuxSqlParser#parse(String, boolean)}. This is the main
   *                     statement from {@link StatementAndSetContext#getMainStatement()}.
   * @param queryContext query context including {@link StatementAndSetContext#getSetContext()}
   * @param hook         calcite planner hook
   */
  public RobuxPlanner createPlanner(
      final SqlEngine engine,
      final String sql,
      final SqlNode sqlNode,
      final Map<String, Object> queryContext,
      final PlannerHook hook
  )
  {
    final PlannerContext context = PlannerContext.create(
        this,
        sql,
        sqlNode,
        engine,
        queryContext,
        hook
    );
    context.dispatchHook(RobuxHook.SQL, sql);

    return new RobuxPlanner(buildFrameworkConfig(context), context, engine, hook);
  }

  /**
   * Not just visible for, but only for testing. Create a planner pre-loaded with an escalated authentication result
   * and ready to go authorization result.
   */
  @VisibleForTesting
  public RobuxPlanner createPlannerForTesting(
      final SqlEngine engine,
      final String sql,
      final Map<String, Object> queryContext
  )
  {
    final StatementAndSetContext statementAndSetContext = RobuxSqlParser.parse(sql, true);
    final RobuxPlanner thePlanner = createPlanner(
        engine,
        sql,
        statementAndSetContext.getMainStatement(),
        statementAndSetContext.getSetContext().isEmpty()
        ? queryContext
        : QueryContexts.override(queryContext, statementAndSetContext.getSetContext()),
        null
    );
    thePlanner.getPlannerContext()
              .setAuthenticationResult(NoopEscalator.getInstance().createEscalatedAuthenticationResult());
    thePlanner.validate();
    thePlanner.authorize(ra -> AuthorizationResult.ALLOW_NO_RESTRICTION, ImmutableSet.of());
    return thePlanner;
  }

  public AuthorizerMapper getAuthorizerMapper()
  {
    return authorizerMapper;
  }

  private FrameworkConfig buildFrameworkConfig(PlannerContext plannerContext)
  {
    final SqlToRelConverter.Config sqlToRelConverterConfig = SqlToRelConverter
        .config()
        .withExpand(false)
        .withDecorrelationEnabled(false)
        .withTrimUnusedFields(false)
        .withInSubQueryThreshold(
            plannerContext.queryContext().getInSubQueryThreshold()
        );

    Frameworks.ConfigBuilder frameworkConfigBuilder = Frameworks
        .newConfigBuilder()
        .parserConfig(RobuxSqlParser.PARSER_CONFIG)
        .traitDefs(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE)
        .convertletTable(new RobuxConvertletTable(plannerContext))
        .operatorTable(operatorTable)
        .programs(calciteRuleManager.programs(plannerContext))
        .executor(new RobuxRexExecutor(plannerContext))
        .typeSystem(RobuxTypeSystem.INSTANCE)
        .defaultSchema(rootSchema.getSubSchema(robuxSchemaName))
        .sqlToRelConverterConfig(sqlToRelConverterConfig)
        .context(new Context()
        {
          @Override
          @SuppressWarnings("unchecked")
          public <C> C unwrap(final Class<C> aClass)
          {
            if (aClass.equals(CalciteConnectionConfig.class)) {
              // This seems to be the best way to provide our own SqlConformance instance. Otherwise, Calcite's
              // validator will not respect it.
              final Properties props = new Properties();
              return (C) new CalciteConnectionConfigImpl(props)
              {
                @Override
                public <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem)
                {
                  return (T) RobuxTypeSystem.INSTANCE;
                }

                @Override
                public SqlConformance conformance()
                {
                  return RobuxConformance.instance();
                }
              };
            }
            if (aClass.equals(PlannerContext.class)) {
              return (C) plannerContext;
            }

            return null;
          }
        });

    if (QueryContexts.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED
        .equals(plannerConfig().getNativeQuerySqlPlanningMode())
    ) {
      frameworkConfigBuilder.costFactory(new RobuxVolcanoCost.Factory());
    }

    return frameworkConfigBuilder.build();

  }

  static class RobuxCalciteConnectionConfigImpl extends CalciteConnectionConfigImpl
  {
    public RobuxCalciteConnectionConfigImpl(Properties properties)
    {
      super(properties);
    }

    @Override
    public <T> T typeSystem(Class<T> typeSystemClass, T defaultTypeSystem)
    {
      return (T) RobuxTypeSystem.INSTANCE;
    }

    @Override
    public SqlConformance conformance()
    {
      return RobuxConformance.instance();
    }

    @Override
    public CalciteConnectionConfigImpl set(CalciteConnectionProperty property, String value)
    {
      final Properties newProperties = (Properties) properties.clone();
      newProperties.setProperty(property.camelName(), value);
      return new RobuxCalciteConnectionConfigImpl(newProperties);
    }
  }
}


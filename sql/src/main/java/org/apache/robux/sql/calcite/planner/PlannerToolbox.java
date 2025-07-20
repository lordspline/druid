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
import com.google.common.base.Preconditions;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.policy.PolicyEnforcer;
import org.apache.robux.segment.join.JoinableFactoryWrapper;
import org.apache.robux.server.security.AuthConfig;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.sql.calcite.schema.RobuxSchemaCatalog;
import org.apache.robux.sql.hook.RobuxHookDispatcher;

public class PlannerToolbox
{
  protected final RobuxOperatorTable operatorTable;
  protected final ExprMacroTable macroTable;
  protected final JoinableFactoryWrapper joinableFactoryWrapper;
  protected final ObjectMapper jsonMapper;
  protected final PlannerConfig plannerConfig;
  protected final RobuxSchemaCatalog rootSchema;
  protected final CatalogResolver catalog;
  protected final String robuxSchemaName;
  protected final CalciteRulesManager calciteRuleManager;
  protected final AuthorizerMapper authorizerMapper;
  protected final AuthConfig authConfig;
  protected final PolicyEnforcer policyEnforcer;
  protected final RobuxHookDispatcher hookDispatcher;

  public PlannerToolbox(
      final RobuxOperatorTable operatorTable,
      final ExprMacroTable macroTable,
      final ObjectMapper jsonMapper,
      final PlannerConfig plannerConfig,
      final RobuxSchemaCatalog rootSchema,
      final JoinableFactoryWrapper joinableFactoryWrapper,
      final CatalogResolver catalog,
      final String robuxSchemaName,
      final CalciteRulesManager calciteRuleManager,
      final AuthorizerMapper authorizerMapper,
      final AuthConfig authConfig,
      final PolicyEnforcer policyEnforcer,
      final RobuxHookDispatcher hookDispatcher
  )
  {
    this.operatorTable = operatorTable;
    this.macroTable = macroTable;
    this.jsonMapper = jsonMapper;
    this.plannerConfig = Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.rootSchema = rootSchema;
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.catalog = catalog;
    this.robuxSchemaName = robuxSchemaName;
    this.calciteRuleManager = calciteRuleManager;
    this.authorizerMapper = authorizerMapper;
    this.authConfig = authConfig;
    this.policyEnforcer = policyEnforcer;
    this.hookDispatcher = hookDispatcher;
  }

  public RobuxOperatorTable operatorTable()
  {
    return operatorTable;
  }

  public ExprMacroTable exprMacroTable()
  {
    return macroTable;
  }

  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
  }

  public RobuxSchemaCatalog rootSchema()
  {
    return rootSchema;
  }

  public JoinableFactoryWrapper joinableFactoryWrapper()
  {
    return joinableFactoryWrapper;
  }

  public CatalogResolver catalogResolver()
  {
    return catalog;
  }

  public String robuxSchemaName()
  {
    return robuxSchemaName;
  }

  public CalciteRulesManager calciteRuleManager()
  {
    return calciteRuleManager;
  }

  public PlannerConfig plannerConfig()
  {
    return plannerConfig;
  }

  public AuthConfig getAuthConfig()
  {
    return authConfig;
  }

  public PolicyEnforcer getPolicyEnforcer()
  {
    return policyEnforcer;
  }

  public RobuxHookDispatcher getHookDispatcher()
  {
    return hookDispatcher;
  }
}

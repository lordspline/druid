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

package org.apache.robux.msq.dart.controller.sql;

import org.apache.robux.msq.exec.QueryKitBasedMSQPlanner;
import org.apache.robux.msq.exec.ResultsContext;
import org.apache.robux.msq.indexing.LegacyMSQSpec;
import org.apache.robux.msq.indexing.QueryDefMSQSpec;
import org.apache.robux.msq.kernel.QueryDefinition;
import org.apache.robux.msq.logical.RobuxLogicalToQueryDefinitionTranslator;
import org.apache.robux.msq.logical.StageMaker;
import org.apache.robux.msq.logical.stages.LogicalStage;
import org.apache.robux.msq.sql.MSQTaskQueryMaker;
import org.apache.robux.query.QueryContext;
import org.apache.robux.query.QueryContexts;
import org.apache.robux.server.QueryResponse;
import org.apache.robux.server.security.ForbiddenException;
import org.apache.robux.sql.calcite.planner.ColumnMappings;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.planner.QueryUtils;
import org.apache.robux.sql.calcite.rel.RobuxQuery;
import org.apache.robux.sql.calcite.rel.logical.RobuxLogicalNode;
import org.apache.robux.sql.calcite.run.QueryMaker;

import java.util.List;
import java.util.Map.Entry;

/**
 * Executes Dart queries with up-front planned {@link QueryDefinition}.
 * <p>
 * Normal execution flow utilizes {@code QueryKit} to prdocue the plan;
 * meanwhile it also supports planning the {@link QueryDefinition} directly from
 * the {@link RobuxLogicalNode}.
 */
class PrePlannedDartQueryMaker implements QueryMaker, QueryMaker.FromRobuxLogical
{
  private PlannerContext plannerContext;
  private DartQueryMaker dartQueryMaker;

  public PrePlannedDartQueryMaker(PlannerContext plannerContext, DartQueryMaker queryMaker)
  {
    this.plannerContext = plannerContext;
    this.dartQueryMaker = queryMaker;
  }

  @Override
  public QueryResponse<Object[]> runQuery(RobuxLogicalNode rootRel)
  {
    if (!plannerContext.getAuthorizationResult().allowAccessWithNoRestriction()) {
      throw new ForbiddenException(plannerContext.getAuthorizationResult().getErrorMessage());
    }
    RobuxLogicalToQueryDefinitionTranslator qdt = new RobuxLogicalToQueryDefinitionTranslator(plannerContext);
    LogicalStage logicalStage = qdt.translate(rootRel);

    StageMaker maker = new StageMaker(plannerContext);
    maker.buildStage(logicalStage);
    QueryDefinition queryDef = maker.buildQueryDefinition();

    QueryContext context = plannerContext.queryContext();
    ColumnMappings columnMappings = QueryUtils.buildColumnMappings(dartQueryMaker.fieldMapping, logicalStage.getLogicalRowSignature());
    QueryDefMSQSpec querySpec = MSQTaskQueryMaker.makeQueryDefMSQSpec(
        null,
        context,
        columnMappings,
        plannerContext,
        null,
        queryDef
    );

    ResultsContext resultsContext = MSQTaskQueryMaker.makeSimpleResultContext(
        querySpec.getQueryDef(), rootRel.getRowType(), dartQueryMaker.fieldMapping, plannerContext
    );
    QueryResponse<Object[]> response = dartQueryMaker.runQueryDefMSQSpec(querySpec, context, resultsContext);
    return response;
  }

  @Override
  public QueryResponse<Object[]> runQuery(RobuxQuery robuxQuery)
  {
    QueryContext queryContext = robuxQuery.getQuery().context();
    ResultsContext resultsContext = DartQueryMaker.makeResultsContext(robuxQuery, dartQueryMaker.fieldMapping, plannerContext);
    QueryDefMSQSpec msqSpec = buildMSQSpec(robuxQuery, dartQueryMaker.fieldMapping, queryContext, resultsContext);
    QueryResponse<Object[]> response = dartQueryMaker.runQueryDefMSQSpec(msqSpec, queryContext, resultsContext);
    return response;
  }

  private QueryDefMSQSpec buildMSQSpec(
      RobuxQuery robuxQuery,
      List<Entry<Integer, String>> fieldMapping,
      QueryContext queryContext,
      ResultsContext resultsContext)
  {
    ColumnMappings columnMappings = QueryUtils.buildColumnMappings(fieldMapping, robuxQuery.getOutputRowSignature());
    LegacyMSQSpec querySpec = MSQTaskQueryMaker.makeLegacyMSQSpec(
        null,
        robuxQuery,
        robuxQuery.getQuery().context(),
        columnMappings,
        plannerContext,
        null
    );

    String dartQueryId = queryContext.getString(QueryContexts.CTX_DART_QUERY_ID);

    QueryDefinition queryDef = new QueryKitBasedMSQPlanner(
        querySpec,
        resultsContext,
        querySpec.getQuery(),
        plannerContext.getJsonMapper(),
        dartQueryMaker.queryKitSpecFactory.makeQueryKitSpec(
            QueryKitBasedMSQPlanner
                .makeQueryControllerToolKit(querySpec.getContext(), plannerContext.getJsonMapper()),
            dartQueryId,
            querySpec.getTuningConfig(),
            querySpec.getContext()
        )
    ).makeQueryDefinition();

    return MSQTaskQueryMaker.makeQueryDefMSQSpec(
        null,
        robuxQuery.getQuery().context(),
        columnMappings,
        plannerContext,
        null,
        queryDef
    );
  }
}

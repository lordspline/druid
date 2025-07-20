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

package org.apache.robux.sql.avatica;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.lang3.RegExUtils;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.msq.guice.MultiStageQuery;
import org.apache.robux.msq.indexing.report.MSQResultsReport.ColumnAndType;
import org.apache.robux.msq.indexing.report.MSQTaskReport;
import org.apache.robux.msq.indexing.report.MSQTaskReportPayload;
import org.apache.robux.msq.test.MSQTestBase;
import org.apache.robux.msq.test.MSQTestOverlordServiceClient;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.server.security.AuthenticatorMapper;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.calcite.planner.RobuxTypeSystem;
import org.apache.robux.sql.calcite.table.RowSignatures;
import org.apache.robux.sql.hook.RobuxHook;
import org.apache.robux.sql.hook.RobuxHookDispatcher;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

@LazySingleton
public class MSQRobuxMeta extends RobuxMeta
{
  protected final MSQTestOverlordServiceClient overlordClient;
  protected final ObjectMapper objectMapper;
  protected final RobuxHookDispatcher hookDispatcher;

  @Inject
  public MSQRobuxMeta(
      final @MultiStageQuery SqlStatementFactory sqlStatementFactory,
      final AvaticaServerConfig config,
      final ErrorHandler errorHandler,
      final AuthenticatorMapper authMapper,
      final MSQTestOverlordServiceClient overlordClient,
      final ObjectMapper objectMapper,
      final RobuxHookDispatcher hookDispatcher)
  {
    super(sqlStatementFactory, config, errorHandler, authMapper);
    this.overlordClient = overlordClient;
    this.objectMapper = objectMapper;
    this.hookDispatcher = hookDispatcher;
  }

  @Override
  protected ExecuteResult doFetch(AbstractRobuxJdbcStatement robuxStatement, int maxRows)
  {
    String taskId = extractTaskId(robuxStatement);


    MSQTaskReportPayload payload = (MSQTaskReportPayload) overlordClient.getReportForTask(taskId)
        .get(MSQTaskReport.REPORT_KEY)
        .getPayload();
    if (payload.getStatus().getStatus().isFailure()) {
      throw new ISE(
          "Query task [%s] failed due to %s",
          taskId,
          payload.getStatus().getErrorReport().toString()
      );
    }

    if (!payload.getStatus().getStatus().isComplete()) {
      throw new ISE("Query task [%s] should have finished", taskId);
    }
    final List<?> resultRows = MSQTestBase.getRows(payload.getResults());
    if (resultRows == null) {
      throw new ISE("Results report not present in the task's report payload");
    }
    try {
      String str = objectMapper
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(payload.getStages());

      str = maskMSQPlan(str, taskId);

      hookDispatcher.dispatch(RobuxHook.MSQ_PLAN, str);
    }
    catch (JsonProcessingException e) {
      hookDispatcher.dispatch(RobuxHook.MSQ_PLAN, "error happened during json serialization");
    }

    Signature signature = makeSignature(robuxStatement, payload.getResults().getSignature());
    @SuppressWarnings("unchecked")
    Frame firstFrame = Frame.create(0, true, (List<Object>) resultRows);
    overlordClient.closeTask(taskId);
    return new ExecuteResult(
        ImmutableList.of(
            MetaResultSet.create(
                robuxStatement.connectionId,
                robuxStatement.statementId,
                false,
                signature,
                firstFrame
            )
        )
    );
  }

  private static final Map<Pattern, String> REPLACEMENT_MAP = ImmutableMap.<Pattern, String>builder()
      .put(Pattern.compile("\"startTime\" : .*"), "\"startTime\" : __TIMESTAMP__")
      .put(Pattern.compile("\"duration\" : .*"), "\"duration\" : __DURATION__")
      .put(Pattern.compile("\"sqlQueryId\" : .*"), "\"sqlQueryId\" : __SQL_QUERY_ID__")
      .build();

  private String maskMSQPlan(String str, String taskId)
  {
    Pattern taskIdPattern = Pattern.compile(Pattern.quote(taskId));
    str = RegExUtils.replaceAll(str, taskIdPattern, "<taskId>");
    for (Entry<Pattern, String> entry : REPLACEMENT_MAP.entrySet()) {
      str = RegExUtils.replaceAll(str, entry.getKey(), entry.getValue());
    }
    return str;
  }

  private Signature makeSignature(AbstractRobuxJdbcStatement robuxStatement, List<ColumnAndType> cat)
  {
    RowSignature sig = ColumnAndType.toRowSignature(cat);
    RelDataType rowType = RowSignatures.toRelDataType(sig, RobuxTypeSystem.TYPE_FACTORY);
    return Meta.Signature.create(
        AbstractRobuxJdbcStatement.createColumnMetaData(rowType),
        robuxStatement.getSqlQuery().sql(),
        Collections.emptyList(),
        Meta.CursorFactory.ARRAY,
        Meta.StatementType.SELECT
    );

  }

  private String extractTaskId(AbstractRobuxJdbcStatement robuxStatement)
  {
    ExecuteResult r = super.doFetch(robuxStatement, 2);
    Object[] row = (Object[]) r.resultSets.get(0).firstFrame.rows.iterator().next();
    return (String) row[0];

  }

}

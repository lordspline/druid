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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.tools.ValidationException;
import org.apache.robux.common.utils.IdUtils;
import org.apache.robux.error.RobuxException;
import org.apache.robux.error.InvalidSqlInput;
import org.apache.robux.java.util.common.granularity.Granularity;
import org.apache.robux.query.explain.ExplainAttributes;
import org.apache.robux.server.security.Action;
import org.apache.robux.server.security.Resource;
import org.apache.robux.server.security.ResourceAction;
import org.apache.robux.server.security.ResourceType;
import org.apache.robux.sql.calcite.parser.RobuxSqlIngest;
import org.apache.robux.sql.calcite.parser.RobuxSqlInsert;
import org.apache.robux.sql.calcite.parser.RobuxSqlParserUtils;
import org.apache.robux.sql.calcite.parser.RobuxSqlReplace;
import org.apache.robux.sql.calcite.parser.ExternalDestinationSqlIdentifier;
import org.apache.robux.sql.calcite.run.EngineFeature;
import org.apache.robux.sql.calcite.run.QueryMaker;
import org.apache.robux.sql.destination.ExportDestination;
import org.apache.robux.sql.destination.IngestDestination;
import org.apache.robux.sql.destination.TableDestination;
import org.apache.robux.storage.ExportStorageProvider;

import java.util.List;

public abstract class IngestHandler extends QueryHandler
{
  protected Granularity ingestionGranularity;
  protected IngestDestination targetDatasource;

  private SqlNode validatedQueryNode;

  IngestHandler(
      HandlerContext handlerContext,
      SqlExplain explain
  )
  {
    super(handlerContext, explain);
  }

  protected static SqlNode convertSourceQuery(RobuxSqlIngest sqlNode)
  {
    SqlNode query = sqlNode.getSource();

    // Check if ORDER BY clause is not provided to the underlying query
    if (query instanceof SqlOrderBy) {
      SqlOrderBy sqlOrderBy = (SqlOrderBy) query;
      SqlNodeList orderByList = sqlOrderBy.orderList;
      if (!(orderByList == null || orderByList.equals(SqlNodeList.EMPTY))) {
        throw InvalidSqlInput.exception(
            "Cannot use an ORDER BY clause on a Query of type [%s], use CLUSTERED BY instead",
            sqlNode.getOperator().getName()
        );
      }
    }
    if (sqlNode.getClusteredBy() != null) {
      query = RobuxSqlParserUtils.convertClusterByToOrderBy(query, sqlNode.getClusteredBy());
    }

    if (!query.isA(SqlKind.QUERY)) {
      throw InvalidSqlInput.exception("Unexpected SQL statement type [%s], expected it to be a QUERY", query.getKind());
    }

    return query;
  }

  protected String operationName()
  {
    return ingestNode().getOperator().getName();
  }

  protected abstract RobuxSqlIngest ingestNode();

  private void validateExport()
  {
    if (!handlerContext.plannerContext().featureAvailable(EngineFeature.WRITE_EXTERNAL_DATA)) {
      throw InvalidSqlInput.exception(
          "Writing to external sources are not supported by requested SQL engine [%s], consider using MSQ.",
          handlerContext.engine().name()
      );
    }

    if (ingestNode().getPartitionedBy() != null) {
      throw RobuxException.forPersona(RobuxException.Persona.USER)
                          .ofCategory(RobuxException.Category.UNSUPPORTED)
                          .build("Export statements do not support a PARTITIONED BY or CLUSTERED BY clause.");
    }

    final SqlIdentifier exportFileFormat = ingestNode().getExportFileFormat();
    if (exportFileFormat == null) {
      throw InvalidSqlInput.exception(
          "Exporting rows into an EXTERN destination requires an AS clause to specify the format, but none was found.",
          operationName()
      );
    } else {
      handlerContext.plannerContext().queryContextMap().put(
          RobuxSqlIngest.SQL_EXPORT_FILE_FORMAT,
          exportFileFormat.toString()
      );
    }
  }

  @Override
  public void validate()
  {
    if (ingestNode().getTargetTable() instanceof ExternalDestinationSqlIdentifier) {
      validateExport();
    } else {
      if (ingestNode().getExportFileFormat() != null) {
        throw InvalidSqlInput.exception(
            "The AS <format> clause should only be specified while exporting rows into an EXTERN destination.",
            operationName()
        );
      }
    }

    // Check if CTX_SQL_OUTER_LIMIT is specified and fail the query if it is. CTX_SQL_OUTER_LIMIT being provided causes
    // the number of rows inserted to be limited which is likely to be confusing and unintended.
    if (handlerContext.queryContextMap().get(PlannerContext.CTX_SQL_OUTER_LIMIT) != null) {
      throw InvalidSqlInput.exception(
          "Context parameter [%s] cannot be provided on operator [%s]",
          PlannerContext.CTX_SQL_OUTER_LIMIT,
          operationName()
      );
    }
    RobuxSqlIngest ingestNode = ingestNode();
    RobuxSqlIngest validatedNode = (RobuxSqlIngest) validate(ingestNode);
    validatedQueryNode = validatedNode.getSource();
    // This context key is set during validation in
    // org.apache.robux.sql.calcite.planner.RobuxSqlValidator.validateInsert.
    String effectiveGranularity = (String) handlerContext.queryContextMap()
        .get(RobuxSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY);
    try {
      ingestionGranularity = effectiveGranularity != null
          ? handlerContext.jsonMapper().readValue(effectiveGranularity, Granularity.class)
          : null;
    }
    catch (JsonProcessingException e) {
      // this should never happen, since the granularity value is validated before being written to contextMap.
      throw InvalidSqlInput.exception(e, "Invalid partition granularity [%s]", effectiveGranularity);
    }
    targetDatasource = validateAndGetDataSourceForIngest();
  }

  @Override
  protected SqlNode validatedQueryNode()
  {
    return validatedQueryNode;
  }

  @Override
  protected RelDataType returnedRowType()
  {
    final RelDataTypeFactory typeFactory = rootQueryRel.rel.getCluster().getTypeFactory();
    return handlerContext.engine().resultTypeForInsert(
        typeFactory,
        rootQueryRel.validatedRowType,
        handlerContext.queryContextMap()
    );
  }

  /**
   * Extract target destination from a {@link SqlInsert}, validates that the ingestion is of a form we support, and
   * adds the resource action required (if the destination is a robux datasource).
   * Expects the target datasource to be an unqualified name, a name qualified by the default schema or an external
   * destination.
   */
  private IngestDestination validateAndGetDataSourceForIngest()
  {
    final SqlInsert insert = ingestNode();

    final SqlIdentifier tableIdentifier = (SqlIdentifier) insert.getTargetTable();
    final IngestDestination dataSource;

    if (tableIdentifier instanceof ExternalDestinationSqlIdentifier) {
      ExternalDestinationSqlIdentifier externalDestination = ((ExternalDestinationSqlIdentifier) tableIdentifier);
      ExportStorageProvider storageProvider = externalDestination.toExportStorageProvider(handlerContext.jsonMapper());
      dataSource = new ExportDestination(storageProvider);
      resourceActions.add(new ResourceAction(new Resource(externalDestination.getDestinationType(), ResourceType.EXTERNAL), Action.WRITE));
    } else if (tableIdentifier.names.size() == 1) {
      // Unqualified name.
      String tableName = Iterables.getOnlyElement(tableIdentifier.names);
      IdUtils.validateId("table", tableName);
      dataSource = new TableDestination(tableName);
      resourceActions.add(new ResourceAction(new Resource(tableName, ResourceType.DATASOURCE), Action.WRITE));
    } else {
      // Qualified name.
      final String defaultSchemaName =
          Iterables.getOnlyElement(CalciteSchema.from(handlerContext.defaultSchema()).path(null));

      if (tableIdentifier.names.size() == 2 && defaultSchemaName.equals(tableIdentifier.names.get(0))) {
        String tableName = tableIdentifier.names.get(1);
        IdUtils.validateId("table", tableName);
        dataSource = new TableDestination(tableName);
        resourceActions.add(new ResourceAction(new Resource(tableName, ResourceType.DATASOURCE), Action.WRITE));
      } else {
        throw InvalidSqlInput.exception(
            "Table [%s] does not support operation [%s] because it is not a Robux datasource",
            tableIdentifier,
            operationName()
        );
      }
    }

    return dataSource;
  }

  @Override
  protected PlannerResult planForRobux() throws ValidationException
  {
    return planWithRobuxConvention();
  }

  @Override
  protected QueryMaker buildQueryMaker(final RelRoot rootQueryRel) throws ValidationException
  {
    return handlerContext.engine().buildQueryMakerForInsert(
        targetDatasource,
        rootQueryRel,
        handlerContext.plannerContext()
    );
  }

  /**
   * Handler for the INSERT statement.
   */
  protected static class InsertHandler extends IngestHandler
  {
    private final RobuxSqlInsert sqlNode;

    public InsertHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        RobuxSqlInsert sqlNode,
        SqlExplain explain
    )
    {
      super(handlerContext, explain);
      this.sqlNode = convertQuery(sqlNode);
      handlerContext.hook().captureInsert(sqlNode);
    }

    protected static RobuxSqlInsert convertQuery(RobuxSqlIngest sqlNode)
    {
      SqlNode query = convertSourceQuery(sqlNode);

      return RobuxSqlInsert.create(new SqlInsert(
              sqlNode.getParserPosition(),
              (SqlNodeList) sqlNode.getOperandList().get(0),
              sqlNode.getOperandList().get(1),
              query,
              (SqlNodeList) sqlNode.getOperandList().get(3)),
          sqlNode.getPartitionedBy(),
          sqlNode.getClusteredBy(),
          sqlNode.getExportFileFormat());
    }

    @Override
    protected RobuxSqlIngest ingestNode()
    {
      return sqlNode;
    }

    @Override
    public void validate()
    {
      if (!handlerContext.plannerContext().featureAvailable(EngineFeature.CAN_INSERT)) {
        throw InvalidSqlInput.exception(
            "INSERT operations are not supported by requested SQL engine [%s], consider using MSQ.",
            handlerContext.engine().name()
        );
      }
      super.validate();
    }

    @Override
    public ExplainAttributes explainAttributes()
    {
      return new ExplainAttributes(
          RobuxSqlInsert.OPERATOR.getName(),
          targetDatasource.getDestinationName(),
          ingestionGranularity,
          RobuxSqlParserUtils.resolveClusteredByColumnsToOutputColumns(sqlNode.getClusteredBy(), rootQueryRel.fields),
          null
      );
    }
  }

  /**
   * Handler for the REPLACE statement.
   */
  protected static class ReplaceHandler extends IngestHandler
  {
    private final RobuxSqlReplace sqlNode;
    private String replaceIntervals;

    public ReplaceHandler(
        SqlStatementHandler.HandlerContext handlerContext,
        RobuxSqlReplace sqlNode,
        SqlExplain explain
    )
    {
      super(
          handlerContext,
          explain
      );
      this.sqlNode = convertQuery(sqlNode);
      handlerContext.hook().captureInsert(sqlNode);
    }

    protected static RobuxSqlReplace convertQuery(RobuxSqlReplace sqlNode)
    {
      SqlNode query = convertSourceQuery(sqlNode);
      return RobuxSqlReplace.create(
          new SqlInsert(
              sqlNode.getParserPosition(),
              (SqlNodeList) sqlNode.getOperandList().get(0),
              sqlNode.getOperandList().get(1),
              query,
              (SqlNodeList) sqlNode.getOperandList().get(3)
          ),
          sqlNode.getPartitionedBy(),
          sqlNode.getClusteredBy(),
          sqlNode.getExportFileFormat(),
          sqlNode.getReplaceTimeQuery()
      );
    }

    @Override
    protected RobuxSqlIngest ingestNode()
    {
      return sqlNode;
    }

    @Override
    public void validate()
    {
      if (ingestNode().getTargetTable() instanceof ExternalDestinationSqlIdentifier) {
        throw InvalidSqlInput.exception(
            "REPLACE operations do no support EXTERN destinations. Use INSERT statements to write to an external destination."
        );
      }
      if (!handlerContext.plannerContext().featureAvailable(EngineFeature.CAN_REPLACE)) {
        throw InvalidSqlInput.exception(
            "REPLACE operations are not supported by the requested SQL engine [%s].  Consider using MSQ.",
            handlerContext.engine().name()
        );
      }
      SqlNode replaceTimeQuery = sqlNode.getReplaceTimeQuery();
      if (replaceTimeQuery == null) {
        throw InvalidSqlInput.exception(
            "Missing time chunk information in OVERWRITE clause for REPLACE. Use "
            + "OVERWRITE WHERE <__time based condition> or OVERWRITE ALL to overwrite the entire table."
        );
      }

      super.validate();
      List<String> replaceIntervalsList = RobuxSqlParserUtils.validateQueryAndConvertToIntervals(
          replaceTimeQuery,
          ingestionGranularity,
          handlerContext.timeZone()
      );
      if (replaceIntervalsList != null) {
        replaceIntervals = String.join(",", replaceIntervalsList);
        handlerContext.queryContextMap().put(
            RobuxSqlReplace.SQL_REPLACE_TIME_CHUNKS,
            replaceIntervals
        );
      }
    }

    @Override
    public ExplainAttributes explainAttributes()
    {
      return new ExplainAttributes(
          RobuxSqlReplace.OPERATOR.getName(),
          targetDatasource.getDestinationName(),
          ingestionGranularity,
          RobuxSqlParserUtils.resolveClusteredByColumnsToOutputColumns(sqlNode.getClusteredBy(), rootQueryRel.fields),
          replaceIntervals
      );
    }
  }
}

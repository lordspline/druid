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

package org.apache.robux.grpc.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.robux.grpc.proto.QueryOuterClass;
import org.apache.robux.grpc.proto.QueryOuterClass.ColumnSchema;
import org.apache.robux.grpc.proto.QueryOuterClass.RobuxType;
import org.apache.robux.grpc.proto.QueryOuterClass.QueryParameter;
import org.apache.robux.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.robux.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.robux.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.robux.java.util.common.RE;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.guava.Accumulator;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryToolChest;
import org.apache.robux.segment.column.ColumnHolder;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.server.QueryLifecycle;
import org.apache.robux.server.QueryLifecycleFactory;
import org.apache.robux.server.security.Access;
import org.apache.robux.server.security.AuthenticationResult;
import org.apache.robux.server.security.AuthorizationResult;
import org.apache.robux.server.security.ForbiddenException;
import org.apache.robux.sql.DirectStatement;
import org.apache.robux.sql.DirectStatement.ResultSet;
import org.apache.robux.sql.SqlPlanningException;
import org.apache.robux.sql.SqlQueryPlus;
import org.apache.robux.sql.SqlRowTransformer;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.calcite.table.RowSignatures;
import org.apache.robux.sql.http.ResultFormat;
import org.apache.robux.sql.http.SqlParameter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;


/**
 * "Driver" for the gRPC query endpoint. Handles translating the gRPC {@link QueryRequest}
 * into Robux's internal formats, running the query, and translating the results into a
 * gRPC {@link QueryResponse}. Allows for easier unit testing as we separate the machinery
 * of running a query, given the request, from the gRPC server machinery.
 */
public class QueryDriver
{
  private static final Logger log = new Logger(QueryDriver.class);

  private static final String TIME_FIELD_KEY = "timeFieldKey";

  /**
   * Internal runtime exception to report request errors.
   */
  protected static class RequestError extends RE
  {
    public RequestError(String msg, Object... args)
    {
      super(msg, args);
    }
  }

  private final ObjectMapper jsonMapper;
  private final SqlStatementFactory sqlStatementFactory;
  private final QueryLifecycleFactory queryLifecycleFactory;

  public QueryDriver(
      final ObjectMapper jsonMapper,
      final SqlStatementFactory sqlStatementFactory,
      final QueryLifecycleFactory queryLifecycleFactory
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.sqlStatementFactory = Preconditions.checkNotNull(sqlStatementFactory, "sqlStatementFactory");
    this.queryLifecycleFactory = queryLifecycleFactory;
  }

  /**
   * First-cut synchronous query handler. Robux prefers to stream results, in
   * part to avoid overly-short network timeouts. However, for now, we simply run
   * the query within this call and prepare the Protobuf response. Async handling
   * can come later.
   */
  public QueryResponse submitQuery(QueryRequest request, AuthenticationResult authResult)
  {
    if (request.getQueryType() == QueryOuterClass.QueryType.NATIVE) {
      return runNativeQuery(request, authResult);
    } else {
      return runSqlQuery(request, authResult);
    }
  }

  private QueryResponse runNativeQuery(QueryRequest request, AuthenticationResult authResult)
  {
    Query<?> query;
    try {
      query = jsonMapper.readValue(request.getQuery(), Query.class);
    }
    catch (JsonProcessingException e) {
      return QueryResponse.newBuilder()
                          .setQueryId("")
                          .setStatus(QueryStatus.REQUEST_ERROR)
                          .setErrorMessage(e.getMessage())
                          .build();
    }
    if (Strings.isNullOrEmpty(query.getId())) {
      query = query.withId(UUID.randomUUID().toString());
    }

    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();

    final org.apache.robux.server.QueryResponse queryResponse;
    final String currThreadName = Thread.currentThread().getName();
    try {
      queryLifecycle.initialize(query);
      AuthorizationResult authorizationResult = queryLifecycle.authorize(authResult);
      if (!authorizationResult.allowAccessWithNoRestriction()) {
        throw new ForbiddenException(Access.DEFAULT_ERROR_MESSAGE);
      }
      queryResponse = queryLifecycle.execute();

      QueryToolChest queryToolChest = queryLifecycle.getToolChest();

      Sequence<Object[]> sequence = queryToolChest.resultsAsArrays(query, queryResponse.getResults());
      RowSignature rowSignature = queryToolChest.resultArraySignature(query);

      Thread.currentThread().setName(StringUtils.format("grpc-native[%s]", query.getId()));
      final ByteString results = encodeNativeResults(request, sequence, rowSignature);
      return QueryResponse.newBuilder()
                          .setQueryId(query.getId())
                          .setStatus(QueryStatus.OK)
                          .setData(results)
                          .clearErrorMessage()
                          .addAllColumns(encodeNativeColumns(rowSignature, request.getSkipColumnsList()))
                          .build();
    }
    catch (IOException | RuntimeException e) {
      return QueryResponse.newBuilder()
                          .setQueryId(query.getId())
                          .setStatus(QueryStatus.RUNTIME_ERROR)
                          .setErrorMessage(e.getMessage())
                          .build();
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  private QueryResponse runSqlQuery(QueryRequest request, AuthenticationResult authResult)
  {
    final SqlQueryPlus queryPlus;
    try {
      queryPlus = translateQuery(request, authResult);
    }
    catch (RuntimeException e) {
      return QueryResponse.newBuilder()
                          .setQueryId("")
                          .setStatus(QueryStatus.REQUEST_ERROR)
                          .setErrorMessage(e.getMessage())
                          .build();
    }
    final DirectStatement stmt = sqlStatementFactory.directStatement(queryPlus);
    final String currThreadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName(StringUtils.format("grpc-sql[%s]", stmt.sqlQueryId()));
      final ResultSet thePlan = stmt.plan();
      final SqlRowTransformer rowTransformer = thePlan.createRowTransformer();
      final ByteString results = encodeSqlResults(request, thePlan.run().getResults(), rowTransformer);
      stmt.reporter().succeeded(results.size());
      stmt.close();
      return QueryResponse.newBuilder()
                          .setQueryId(stmt.sqlQueryId())
                          .setStatus(QueryStatus.OK)
                          .setData(results)
                          .clearErrorMessage()
                          .addAllColumns(encodeSqlColumns(rowTransformer))
                          .build();
    }
    catch (ForbiddenException e) {
      stmt.reporter().failed(e);
      stmt.close();
      throw e;
    }
    catch (RequestError e) {
      stmt.reporter().failed(e);
      stmt.close();
      return QueryResponse.newBuilder()
                          .setQueryId(stmt.sqlQueryId())
                          .setStatus(QueryStatus.REQUEST_ERROR)
                          .setErrorMessage(e.getMessage())
                          .build();
    }
    catch (SqlPlanningException e) {
      stmt.reporter().failed(e);
      stmt.close();
      return QueryResponse.newBuilder()
                          .setQueryId(stmt.sqlQueryId())
                          .setStatus(QueryStatus.INVALID_SQL)
                          .setErrorMessage(e.getMessage())
                          .build();
    }
    catch (IOException | RuntimeException e) {
      stmt.reporter().failed(e);
      stmt.close();
      return QueryResponse.newBuilder()
                          .setQueryId(stmt.sqlQueryId())
                          .setStatus(QueryStatus.RUNTIME_ERROR)
                          .setErrorMessage(e.getMessage())
                          .build();
    }
    // There is a claim that Calcite sometimes throws a java.lang.AssertionError, but we do not have a test that can
    // reproduce it checked into the code (the best we have is something that uses mocks to throw an Error, which is
    // dubious at best).  We keep this just in case, but it might be best to remove it and see where the
    // AssertionErrors are coming from and do something to ensure that they don't actually make it out of Calcite
    catch (AssertionError e) {
      stmt.reporter().failed(e);
      stmt.close();
      return QueryResponse.newBuilder()
                          .setQueryId(stmt.sqlQueryId())
                          .setStatus(QueryStatus.RUNTIME_ERROR)
                          .setErrorMessage(e.getMessage())
                          .build();
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  /**
   * Convert the rRPC query format to the internal {@link SqlQueryPlus} format.
   */
  private SqlQueryPlus translateQuery(QueryRequest request, AuthenticationResult authResult)
  {
    return SqlQueryPlus.builder()
                       .sql(request.getQuery())
                       .context(translateContext(request))
                       .sqlParameters(translateParameters(request))
                       .auth(authResult)
                       .build();
  }

  /**
   * Translate the query context from the gRPC format to the internal format. When
   * read from REST/JSON, the JSON translator will convert the type of each value
   * into a number, Boolean, etc. gRPC has no similar feature. Rather than clutter up
   * the gRPC request with typed context values, we rely on the existing code that can
   * translate string values to the desired type on the fly. Thus, we build up a
   * {@code Map<String, String>}.
   */
  private Map<String, Object> translateContext(QueryRequest request)
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    if (request.getContextCount() > 0) {
      for (Map.Entry<String, String> entry : request.getContextMap().entrySet()) {
        builder.put(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  /**
   * Convert the gRPC parameter format to the internal Robux {@link SqlParameter}
   * format. That format is then again translated by the {@link SqlQueryPlus} class.
   */
  private List<SqlParameter> translateParameters(QueryRequest request)
  {
    if (request.getParametersCount() == 0) {
      return null;
    }
    List<SqlParameter> params = new ArrayList<>();
    for (QueryParameter value : request.getParametersList()) {
      params.add(translateParameter(value));
    }
    return params;
  }

  private SqlParameter translateParameter(QueryParameter value)
  {
    switch (value.getValueCase()) {
      case DOUBLEVALUE:
        return new SqlParameter(SqlType.DOUBLE, value.getDoubleValue());
      case LONGVALUE:
        return new SqlParameter(SqlType.BIGINT, value.getLongValue());
      case STRINGVALUE:
        return new SqlParameter(SqlType.VARCHAR, value.getStringValue());
      case NULLVALUE:
      case VALUE_NOT_SET:
        return null;
      case ARRAYVALUE:
      default:
        throw new RequestError("Invalid parameter type: " + value.getValueCase().name());
    }
  }

  /**
   * Translate the column schema from the Robux internal form to the gRPC
   * {@link ColumnSchema} form. Note that since the gRPC response returns the
   * schema, none of the data formats include a header. This makes the data format
   * simpler and cleaner.
   */
  private Iterable<? extends ColumnSchema> encodeSqlColumns(SqlRowTransformer rowTransformer)
  {
    RelDataType rowType = rowTransformer.getRowType();
    final RowSignature signature = RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType);
    List<ColumnSchema> cols = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      ColumnSchema col = ColumnSchema.newBuilder()
                                     .setName(signature.getColumnName(i))
                                     .setSqlType(rowType.getFieldList().get(i).getType().getSqlTypeName().getName())
                                     .setRobuxType(convertRobuxType(signature.getColumnType(i)))
                                     .build();
      cols.add(col);
    }
    return cols;
  }

  private Iterable<? extends ColumnSchema> encodeNativeColumns(RowSignature rowSignature, List<String> skipColumns)
  {
    List<ColumnSchema> cols = new ArrayList<>();
    for (int i = 0; i < rowSignature.getColumnNames().size(); i++) {
      if (skipColumns.contains(rowSignature.getColumnName(i))) {
        continue;
      }
      ColumnSchema col = ColumnSchema.newBuilder()
                                     .setName(rowSignature.getColumnName(i))
                                     .setRobuxType(convertRobuxType(rowSignature.getColumnType(i)))
                                     .build();
      cols.add(col);
    }
    return cols;
  }

  /**
   * Convert from Robux's internal format of the Robux data type to the gRPC form.
   */
  private RobuxType convertRobuxType(Optional<ColumnType> colType)
  {
    if (!colType.isPresent()) {
      return RobuxType.UNKNOWN_TYPE;
    }
    ColumnType robuxType = colType.get();
    if (robuxType == ColumnType.STRING) {
      return RobuxType.STRING;
    }
    if (robuxType == ColumnType.STRING_ARRAY) {
      return RobuxType.STRING_ARRAY;
    }
    if (robuxType == ColumnType.LONG) {
      return RobuxType.LONG;
    }
    if (robuxType == ColumnType.LONG_ARRAY) {
      return RobuxType.LONG_ARRAY;
    }
    if (robuxType == ColumnType.FLOAT) {
      return RobuxType.FLOAT;
    }
    if (robuxType == ColumnType.FLOAT_ARRAY) {
      return RobuxType.FLOAT_ARRAY;
    }
    if (robuxType == ColumnType.DOUBLE) {
      return RobuxType.DOUBLE;
    }
    if (robuxType == ColumnType.DOUBLE_ARRAY) {
      return RobuxType.DOUBLE_ARRAY;
    }
    if (robuxType == ColumnType.UNKNOWN_COMPLEX) {
      return RobuxType.COMPLEX;
    }
    return RobuxType.UNKNOWN_TYPE;
  }

  /**
   * Generic mechanism to write query results to one of the supported gRPC formats.
   */
  public interface GrpcResultWriter
  {
    void start() throws IOException;

    void writeRow(Object[] row) throws IOException;

    void close() throws IOException;
  }

  /**
   * Writer for the SQL result formats. Reuses the SQL format writer implementations.
   * Note: gRPC does not use the headers: schema information is available in the
   * rRPC response.
   */
  public static class GrpcSqlResultFormatWriter implements GrpcResultWriter
  {
    protected final ResultFormat.Writer formatWriter;
    protected final SqlRowTransformer rowTransformer;

    public GrpcSqlResultFormatWriter(
        final ResultFormat.Writer formatWriter,
        final SqlRowTransformer rowTransformer
    )
    {
      this.formatWriter = formatWriter;
      this.rowTransformer = rowTransformer;
    }

    @Override
    public void start() throws IOException
    {
      formatWriter.writeResponseStart();
    }

    @Override
    public void writeRow(Object[] row) throws IOException
    {
      formatWriter.writeRowStart();
      for (int i = 0; i < rowTransformer.getFieldList().size(); i++) {
        final Object value;
        if (formatWriter instanceof ProtobufWriter) {
          value = ProtobufTransformer.transform(rowTransformer, row, i);
        } else {
          value = rowTransformer.transform(row, i);
        }
        formatWriter.writeRowField(rowTransformer.getFieldList().get(i), value);
      }
      formatWriter.writeRowEnd();
    }

    @Override
    public void close() throws IOException
    {
      formatWriter.writeResponseEnd();
      formatWriter.close();
    }
  }

  public static class GrpcNativeResultFormatWriter implements GrpcResultWriter
  {
    protected final ResultFormat.Writer formatWriter;
    protected final RowSignature rowSignature;
    private final String timeFieldName;
    private final List<String> timeColumns;
    private final List<String> skipColumns;

    public GrpcNativeResultFormatWriter(
        final ResultFormat.Writer formatWriter,
        final RowSignature rowSignature,
        final String timeFieldName,
        final List<String> timeColumns,
        final List<String> skipColumns
    )
    {
      this.formatWriter = formatWriter;
      this.rowSignature = rowSignature;
      this.timeFieldName = timeFieldName;
      this.timeColumns = timeColumns;
      this.skipColumns = skipColumns;
    }

    @Override
    public void start() throws IOException
    {
      formatWriter.writeResponseStart();
    }

    @Override
    public void writeRow(Object[] row) throws IOException
    {
      formatWriter.writeRowStart();

      for (int i = 0; i < rowSignature.getColumnNames().size(); i++) {

        final String columnName = rowSignature.getColumnName(i);
        if (skipColumns.contains(columnName)) {
          log.debug("Skipping column [%s] from the result.", columnName);
          continue;
        }

        boolean isRobuxTimeColumn = columnName.equals(ColumnHolder.TIME_COLUMN_NAME);
        boolean convertTime = timeColumns.contains(rowSignature.getColumnName(i));

        final Object value;
        if (formatWriter instanceof ProtobufWriter) {
          value = ProtobufTransformer.transform(rowSignature, row, i, convertTime);
        } else {
          if (convertTime) {
            value = ISODateTimeFormat.dateTime().print(((long) row[i]));
          } else {
            value = row[i];
          }
        }
        final String outputColumnName;
        if (isRobuxTimeColumn) {
          outputColumnName = timeFieldName;
        } else {
          outputColumnName = rowSignature.getColumnName(i);
        }
        formatWriter.writeRowField(outputColumnName, value);
      }
      formatWriter.writeRowEnd();
    }


    @Override
    public void close() throws IOException
    {
      formatWriter.writeResponseEnd();
      formatWriter.close();
    }
  }

  /**
   * Internal runtime exception to pass {@link IOException}s though the
   * {@link Sequence} {@link Accumulator} protocol.
   */
  private static class ResponseError extends RuntimeException
  {
    public ResponseError(IOException e)
    {
      super(e);
    }
  }

  /**
   * Robux query results use a complex {@link Sequence} mechanism. This class uses an
   * {@link Accumulator} to walk the results and present each to the associated
   * results writer. This is a very rough analogy of the {@code SqlResourceQueryResultPusher}
   * in the REST {@code SqlResource} class.
   */
  public static class GrpcResultsAccumulator implements Accumulator<Void, Object[]>
  {
    private final GrpcResultWriter writer;

    public GrpcResultsAccumulator(final GrpcResultWriter writer)
    {
      this.writer = writer;
    }

    public void push(Sequence<Object[]> results) throws IOException
    {
      writer.start();
      try {
        results.accumulate(null, this);
      }
      catch (ResponseError e) {
        throw (IOException) e.getCause();
      }
      writer.close();
    }

    @Override
    public Void accumulate(Void accumulated, Object[] in)
    {
      try {
        writer.writeRow(in);
      }
      catch (IOException e) {
        throw new ResponseError(e);
      }
      return null;
    }
  }

  /**
   * Convert the query results to a set of bytes to be attached to the query response.
   * <p>
   * This version is pretty basic: the results are materialized as a byte array. That's
   * fine for small result sets, but should be rethought for larger result sets.
   */
  private ByteString encodeSqlResults(
      final QueryRequest request,
      final Sequence<Object[]> result,
      final SqlRowTransformer rowTransformer
  ) throws IOException
  {
    // Accumulate the results as a byte array.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GrpcResultWriter writer;

    // For the SQL-supported formats, use the SQL-provided writers.
    switch (request.getResultFormat()) {
      case CSV:
        writer = new GrpcSqlResultFormatWriter(
            ResultFormat.CSV.createFormatter(out, jsonMapper),
            rowTransformer
        );
        break;
      case JSON_ARRAY:
        writer = new GrpcSqlResultFormatWriter(
            ResultFormat.ARRAY.createFormatter(out, jsonMapper),
            rowTransformer
        );
        break;
      case JSON_ARRAY_LINES:
        writer = new GrpcSqlResultFormatWriter(
            ResultFormat.ARRAYLINES.createFormatter(out, jsonMapper),
            rowTransformer
        );
        break;
      case PROTOBUF_INLINE:
        writer = new GrpcSqlResultFormatWriter(
            new ProtobufWriter(out, getProtobufClass(request)),
            rowTransformer
        );
        break;
      default:
        throw new RequestError("Unsupported query result format: " + request.getResultFormat().name());
    }
    GrpcResultsAccumulator accumulator = new GrpcResultsAccumulator(writer);
    accumulator.push(result);
    return ByteString.copyFrom(out.toByteArray());
  }

  private ByteString encodeNativeResults(
      final QueryRequest request,
      final Sequence<Object[]> result,
      final RowSignature rowSignature
  ) throws IOException
  {
    // Accumulate the results as a byte array.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GrpcResultWriter writer;
    final String timeFieldName = request.getContextMap().getOrDefault(TIME_FIELD_KEY, "time");
    final List<String> skipColumns = request.getSkipColumnsList();
    final List<String> timeColumns = request.getTimeColumnsList();

    switch (request.getResultFormat()) {
      case CSV:
        writer = new GrpcNativeResultFormatWriter(
            ResultFormat.CSV.createFormatter(out, jsonMapper),
            rowSignature,
            timeFieldName,
            timeColumns,
            skipColumns
        );
        break;
      case JSON_ARRAY:
        writer = new GrpcNativeResultFormatWriter(
            ResultFormat.ARRAY.createFormatter(out, jsonMapper),
            rowSignature,
            timeFieldName,
            timeColumns,
            skipColumns
        );
        break;
      case JSON_ARRAY_LINES:
        writer = new GrpcNativeResultFormatWriter(
            ResultFormat.ARRAYLINES.createFormatter(out, jsonMapper),
            rowSignature,
            timeFieldName,
            timeColumns,
            skipColumns
        );
        break;
      case PROTOBUF_INLINE:
        writer = new GrpcNativeResultFormatWriter(
            new ProtobufWriter(out, getProtobufClass(request)),
            rowSignature,
            timeFieldName,
            timeColumns,
            skipColumns
        );
        break;
      default:
        throw new RequestError("Unsupported query result format: " + request.getResultFormat());
    }
    GrpcResultsAccumulator accumulator = new GrpcResultsAccumulator(writer);
    accumulator.push(result);
    return ByteString.copyFrom(out.toByteArray());
  }

  @SuppressWarnings("unchecked")
  private Class<GeneratedMessageV3> getProtobufClass(final QueryRequest request)
  {
    try {
      return (Class<GeneratedMessageV3>) Class.forName(request.getProtobufMessageName());
    }
    catch (ClassNotFoundException e) {
      throw new RequestError(
          "The Protobuf class [%s] is not known. Is your protobuf jar on the class path?",
          request.getProtobufMessageName()
      );
    }
    catch (ClassCastException e) {
      throw new RequestError(
          "The class [%s] is not a Protobuf",
          request.getProtobufMessageName()
      );
    }
  }
}

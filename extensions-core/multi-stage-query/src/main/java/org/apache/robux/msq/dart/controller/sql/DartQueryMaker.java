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

import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.robux.io.LimitedOutputStream;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.Either;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.guava.BaseSequence;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.jackson.JacksonUtils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.msq.dart.controller.ControllerHolder;
import org.apache.robux.msq.dart.controller.DartControllerContextFactory;
import org.apache.robux.msq.dart.controller.DartControllerRegistry;
import org.apache.robux.msq.dart.guice.DartControllerConfig;
import org.apache.robux.msq.exec.Controller;
import org.apache.robux.msq.exec.ControllerContext;
import org.apache.robux.msq.exec.ControllerImpl;
import org.apache.robux.msq.exec.QueryKitSpecFactory;
import org.apache.robux.msq.exec.QueryListener;
import org.apache.robux.msq.exec.ResultsContext;
import org.apache.robux.msq.indexing.LegacyMSQSpec;
import org.apache.robux.msq.indexing.QueryDefMSQSpec;
import org.apache.robux.msq.indexing.TaskReportQueryListener;
import org.apache.robux.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.robux.msq.indexing.error.CanceledFault;
import org.apache.robux.msq.indexing.error.CancellationReason;
import org.apache.robux.msq.indexing.error.MSQErrorReport;
import org.apache.robux.msq.indexing.report.MSQResultsReport;
import org.apache.robux.msq.indexing.report.MSQStatusReport;
import org.apache.robux.msq.indexing.report.MSQTaskReportPayload;
import org.apache.robux.msq.sql.MSQTaskQueryMaker;
import org.apache.robux.query.QueryContext;
import org.apache.robux.query.QueryContexts;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.server.QueryResponse;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.sql.calcite.planner.ColumnMappings;
import org.apache.robux.sql.calcite.planner.PlannerContext;
import org.apache.robux.sql.calcite.planner.QueryUtils;
import org.apache.robux.sql.calcite.rel.RobuxQuery;
import org.apache.robux.sql.calcite.run.QueryMaker;
import org.apache.robux.sql.calcite.run.SqlResults;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * SQL {@link QueryMaker}. Executes queries in two ways, depending on whether the user asked for a full report.
 *
 * When including a full report, the controller runs in the SQL planning thread (typically an HTTP thread) using
 * the method {@link #runWithReport(ControllerHolder)}. The entire response is buffered in memory, up to
 * {@link DartControllerConfig#getMaxQueryReportSize()}.
 *
 * When not including a full report, the controller runs in {@link #controllerExecutor} and results are streamed
 * back to the user through {@link ResultIterator}. There is no limit to the size of the returned results.
 */
public class DartQueryMaker implements QueryMaker
{
  private static final Logger log = new Logger(DartQueryMaker.class);

  final List<Entry<Integer, String>> fieldMapping;
  private final DartControllerContextFactory controllerContextFactory;
  private final PlannerContext plannerContext;

  /**
   * Controller registry, used to register and remove controllers as they start and finish.
   */
  private final DartControllerRegistry controllerRegistry;

  /**
   * Controller config.
   */
  private final DartControllerConfig controllerConfig;

  /**
   * Executor for {@link #runWithoutReport(ControllerHolder)}. Number of thread is equal to
   * {@link DartControllerConfig#getConcurrentQueries()}, which limits the number of concurrent controllers.
   */
  private final ExecutorService controllerExecutor;
  private final ServerConfig serverConfig;

  final QueryKitSpecFactory queryKitSpecFactory;

  public DartQueryMaker(
      List<Entry<Integer, String>> fieldMapping,
      DartControllerContextFactory controllerContextFactory,
      PlannerContext plannerContext,
      DartControllerRegistry controllerRegistry,
      DartControllerConfig controllerConfig,
      ExecutorService controllerExecutor,
      QueryKitSpecFactory queryKitSpecFactory,
      ServerConfig serverConfig
  )
  {
    this.fieldMapping = fieldMapping;
    this.controllerContextFactory = controllerContextFactory;
    this.plannerContext = plannerContext;
    this.controllerRegistry = controllerRegistry;
    this.controllerConfig = controllerConfig;
    this.controllerExecutor = controllerExecutor;
    this.queryKitSpecFactory = queryKitSpecFactory;
    this.serverConfig = serverConfig;
  }

  @Override
  public QueryResponse<Object[]> runQuery(RobuxQuery robuxQuery)
  {
    ColumnMappings columnMappings = QueryUtils.buildColumnMappings(fieldMapping, robuxQuery.getOutputRowSignature());
    final LegacyMSQSpec querySpec = MSQTaskQueryMaker.makeLegacyMSQSpec(
        null,
        robuxQuery,
        finalizeTimeout(robuxQuery.getQuery().context()),
        columnMappings,
        plannerContext,
        null
    );


    final ResultsContext resultsContext = MSQTaskQueryMaker.makeResultsContext(robuxQuery, fieldMapping, plannerContext);

    return runLegacyMSQSpec(querySpec, robuxQuery.getQuery().context(), resultsContext);
  }

  public static ResultsContext makeResultsContext(RobuxQuery robuxQuery, List<Entry<Integer, String>> fieldMapping,
      PlannerContext plannerContext)
  {
    final List<Pair<SqlTypeName, ColumnType>> types = MSQTaskQueryMaker.getTypes(robuxQuery, fieldMapping, plannerContext);
    final ResultsContext resultsContext = new ResultsContext(
        types.stream().map(p -> p.lhs).collect(Collectors.toList()),
        SqlResults.Context.fromPlannerContext(plannerContext)
    );
    return resultsContext;
  }

  private ControllerImpl makeLegacyController(LegacyMSQSpec querySpec, QueryContext context, ResultsContext resultsContext)
  {
    final ControllerContext controllerContext = controllerContextFactory.newContext(context);

    return new ControllerImpl(
        querySpec,
        resultsContext,
        controllerContext,
        queryKitSpecFactory
    );
  }

  private ControllerImpl makeQueryDefController(QueryDefMSQSpec querySpec, QueryContext context, ResultsContext resultsContext)
  {
    final ControllerContext controllerContext = controllerContextFactory.newContext(context);

    return new ControllerImpl(
        querySpec,
        resultsContext,
        controllerContext,
        queryKitSpecFactory
    );
  }

  public QueryResponse<Object[]> runLegacyMSQSpec(LegacyMSQSpec querySpec, QueryContext context, ResultsContext resultsContext)
  {
    final ControllerImpl controller = makeLegacyController(querySpec, context, resultsContext);
    return runController(controller, context.getFullReport());
  }

  public QueryResponse<Object[]> runQueryDefMSQSpec(QueryDefMSQSpec querySpec, QueryContext context, ResultsContext resultsContext)
  {
    final ControllerImpl controller = makeQueryDefController(querySpec, context, resultsContext);
    return runController(controller, context.getFullReport());
  }

  private QueryResponse<Object[]> runController(final ControllerImpl controller, final boolean fullReport)
  {
    final ControllerHolder controllerHolder = new ControllerHolder(
        controller,
        plannerContext.getSqlQueryId(),
        plannerContext.getSql(),
        plannerContext.getAuthenticationResult(),
        DateTimes.nowUtc()
    );

    // Register controller before submitting anything to controllerExeuctor, so it shows up in
    // "active controllers" lists.
    controllerRegistry.register(controllerHolder);

    try {
      // runWithReport, runWithoutReport are responsible for calling controllerRegistry.deregister(controllerHolder)
      // when their work is done.
      final Sequence<Object[]> results =
          fullReport ? runWithReport(controllerHolder) : runWithoutReport(controllerHolder);
      return QueryResponse.withEmptyContext(results);
    }
    catch (Throwable e) {
      // Error while calling runWithReport or runWithoutReport. Deregister controller immediately.
      controllerRegistry.deregister(controllerHolder);
      throw e;
    }
  }

  /**
   * Adds the timeout parameter to the query context, considering the default and maximum values from
   * {@link ServerConfig}.
   */
  private QueryContext finalizeTimeout(QueryContext queryContext)
  {
    final long timeout = queryContext.getTimeout(serverConfig.getDefaultQueryTimeout());
    QueryContext timeoutContext = queryContext.override(Map.of(QueryContexts.TIMEOUT_KEY, timeout));
    timeoutContext.verifyMaxQueryTimeout(serverConfig.getMaxQueryTimeout());
    return timeoutContext;
  }

  /**
   * Run a query and return the full report, buffered in memory up to
   * {@link DartControllerConfig#getMaxQueryReportSize()}.
   *
   * Arranges for {@link DartControllerRegistry#deregister(ControllerHolder)} to be called upon completion (either
   * success or failure).
   */
  private Sequence<Object[]> runWithReport(final ControllerHolder controllerHolder)
  {
    final Future<Map<String, Object>> reportFuture;

    // Run in controllerExecutor. Control doesn't really *need* to be moved to another thread, but we have to
    // use the controllerExecutor anyway, to ensure we respect the concurrentQueries configuration.
    reportFuture = controllerExecutor.submit(() -> {
      final String threadName = Thread.currentThread().getName();

      try {
        Thread.currentThread().setName(nameThread(plannerContext));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final TaskReportQueryListener queryListener = new TaskReportQueryListener(
            TaskReportMSQDestination.instance(),
            () -> new LimitedOutputStream(
                baos,
                controllerConfig.getMaxQueryReportSize(),
                limit -> StringUtils.format(
                    "maxQueryReportSize[%,d] exceeded. "
                    + "Try limiting the result set for your query, or run it with %s[false]",
                    limit,
                    QueryContexts.CTX_FULL_REPORT
                )
            ),
            plannerContext.getJsonMapper(),
            controllerHolder.getController().queryId(),
            Collections.emptyMap()
        );

        if (controllerHolder.run(queryListener)) {
          return plannerContext.getJsonMapper()
                               .readValue(baos.toByteArray(), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
        } else {
          // Controller was canceled before it ran.
          throw MSQErrorReport
              .fromFault(
                  controllerHolder.getController().queryId(),
                  null,
                  null,
                  CanceledFault.userRequest()
              )
              .toRobuxException();
        }
      }
      finally {
        controllerRegistry.deregister(controllerHolder);
        Thread.currentThread().setName(threadName);
      }
    });

    // Return a sequence that reads one row (the report) from reportFuture.
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<>()
        {
          @Override
          public Iterator<Object[]> make()
          {
            try {
              return Iterators.singletonIterator(new Object[]{reportFuture.get()});
            }
            catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
              // Unwrap ExecutionExceptions, so errors such as RobuxException are serialized properly.
              Throwables.throwIfUnchecked(e.getCause());
              throw new RuntimeException(e.getCause());
            }
          }

          @Override
          public void cleanup(Iterator<Object[]> iterFromMake)
          {
            // Nothing to do.
          }
        }
    );
  }

  /**
   * Run a query and return the results only, streamed back using {@link ResultIteratorMaker}.
   *
   * Arranges for {@link DartControllerRegistry#deregister(ControllerHolder)} to be called upon completion (either
   * success or failure).
   */
  private Sequence<Object[]> runWithoutReport(final ControllerHolder controllerHolder)
  {
    return new BaseSequence<>(new ResultIteratorMaker(controllerHolder));
  }

  /**
   * Generate a name for a thread in {@link #controllerExecutor}.
   */
  private String nameThread(final PlannerContext plannerContext)
  {
    return StringUtils.format(
        "%s-sqlQueryId[%s]-queryId[%s]",
        Thread.currentThread().getName(),
        plannerContext.getSqlQueryId(),
        plannerContext.queryContext().get(QueryContexts.CTX_DART_QUERY_ID)
    );
  }

  /**
   * Helper for {@link #runWithoutReport(ControllerHolder)}.
   */
  class ResultIteratorMaker implements BaseSequence.IteratorMaker<Object[], ResultIterator>
  {
    private final ControllerHolder controllerHolder;
    private final ResultIterator resultIterator = new ResultIterator();
    private boolean made;

    public ResultIteratorMaker(ControllerHolder holder)
    {
      this.controllerHolder = holder;
      submitController();
    }

    /**
     * Submits the controller to the executor in the constructor, and remove it from the registry when the
     * future resolves.
     */
    private void submitController()
    {
      controllerExecutor.submit(() -> {
        final Controller controller = controllerHolder.getController();
        final String threadName = Thread.currentThread().getName();

        try {
          Thread.currentThread().setName(nameThread(plannerContext));

          if (!controllerHolder.run(resultIterator)) {
            // Controller was canceled before it ran. Push a cancellation error to the resultIterator, so the sequence
            // returned by "runWithoutReport" can resolve.
            resultIterator.pushError(
                MSQErrorReport.fromFault(
                    controllerHolder.getController().queryId(),
                    null,
                    null,
                    CanceledFault.userRequest()
                ).toRobuxException()
            );
          }
        }
        catch (Exception e) {
          log.warn(
              e,
              "Controller failed for sqlQueryId[%s], controllerHost[%s]",
              plannerContext.getSqlQueryId(),
              controller.queryId()
          );
        }
        catch (Throwable e) {
          log.error(
              e,
              "Controller failed for sqlQueryId[%s], controllerHost[%s]",
              plannerContext.getSqlQueryId(),
              controller.queryId()
          );
          throw e;
        }
        finally {
          controllerRegistry.deregister(controllerHolder);
          Thread.currentThread().setName(threadName);
        }
      });
    }

    @Override
    public ResultIterator make()
    {
      if (made) {
        throw new ISE("Cannot call make() more than once");
      }

      made = true;
      return resultIterator;
    }

    @Override
    public void cleanup(final ResultIterator iterFromMake)
    {
      if (!iterFromMake.complete) {
        controllerHolder.cancel(CancellationReason.UNKNOWN);
      }
    }
  }

  /**
   * Helper for {@link ResultIteratorMaker}, which is in turn a helper for {@link #runWithoutReport(ControllerHolder)}.
   */
  static class ResultIterator implements Iterator<Object[]>, QueryListener
  {
    /**
     * Number of rows to buffer from {@link #onResultRow(Object[])}.
     */
    private static final int BUFFER_SIZE = 128;

    /**
     * Empty optional signifies results are complete.
     */
    private final BlockingQueue<Either<Throwable, Object[]>> rowBuffer = new ArrayBlockingQueue<>(BUFFER_SIZE);

    /**
     * Only accessed by {@link Iterator} methods, so no need to be thread-safe.
     */
    @Nullable
    private Either<Throwable, Object[]> current;

    private volatile boolean complete;

    @Override
    public boolean hasNext()
    {
      return populateAndReturnCurrent().isPresent();
    }

    @Override
    public Object[] next()
    {
      final Object[] retVal = populateAndReturnCurrent().orElseThrow(NoSuchElementException::new);
      current = null;
      return retVal;
    }

    private Optional<Object[]> populateAndReturnCurrent()
    {
      if (current == null) {
        try {
          current = rowBuffer.take();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }

      if (current.isValue()) {
        return Optional.ofNullable(current.valueOrThrow());
      } else {
        // Don't use valueOrThrow to throw errors; here we *don't* want the wrapping in RuntimeException
        // that Either.valueOrThrow does. We want the original RobuxException to be propagated to the user, if
        // there is one.
        final Throwable e = current.error();
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean readResults()
    {
      return !complete;
    }

    @Override
    public void onResultsStart(
        final List<MSQResultsReport.ColumnAndType> signature,
        @Nullable final List<SqlTypeName> sqlTypeNames
    )
    {
      // Nothing to do.
    }

    @Override
    public boolean onResultRow(Object[] row)
    {
      try {
        rowBuffer.put(Either.value(row));
        return !complete;
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void onResultsComplete()
    {
      // Nothing to do.
    }

    @Override
    public void onQueryComplete(MSQTaskReportPayload report)
    {
      try {
        complete = true;

        final MSQStatusReport statusReport = report.getStatus();

        if (statusReport.getStatus().isSuccess()) {
          rowBuffer.put(Either.value(null));
        } else {
          pushError(statusReport.getErrorReport().toRobuxException());
        }
      }
      catch (InterruptedException e) {
        // Can't fix this by pushing an error, because the rowBuffer isn't accepting new entries.
        // Give up, allow controllerHolder.run() to fail.
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    public void pushError(final Throwable e) throws InterruptedException
    {
      rowBuffer.put(Either.error(e));
    }
  }
}

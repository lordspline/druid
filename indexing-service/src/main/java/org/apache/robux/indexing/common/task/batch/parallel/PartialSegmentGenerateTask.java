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

package org.apache.robux.indexing.common.task.batch.parallel;

import com.google.common.base.Preconditions;
import org.apache.robux.data.input.InputSource;
import org.apache.robux.indexer.IngestionState;
import org.apache.robux.indexer.TaskStatus;
import org.apache.robux.indexer.partitions.PartitionsSpec;
import org.apache.robux.indexer.report.TaskReport;
import org.apache.robux.indexing.common.TaskRealtimeMetricsMonitorBuilder;
import org.apache.robux.indexing.common.TaskToolbox;
import org.apache.robux.indexing.common.stats.TaskRealtimeMetricsMonitor;
import org.apache.robux.indexing.common.task.BatchAppenderators;
import org.apache.robux.indexing.common.task.InputSourceProcessor;
import org.apache.robux.indexing.common.task.SegmentAllocatorForBatch;
import org.apache.robux.indexing.common.task.SequenceNameFunction;
import org.apache.robux.indexing.common.task.TaskResource;
import org.apache.robux.indexing.common.task.batch.parallel.iterator.IndexTaskInputRowIteratorBuilder;
import org.apache.robux.indexing.input.RobuxInputSource;
import org.apache.robux.indexing.input.WindowedSegmentId;
import org.apache.robux.indexing.worker.shuffle.ShuffleDataSegmentPusher;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.segment.SegmentSchemaMapping;
import org.apache.robux.segment.incremental.ParseExceptionHandler;
import org.apache.robux.segment.incremental.ParseExceptionReport;
import org.apache.robux.segment.incremental.RowIngestionMeters;
import org.apache.robux.segment.indexing.DataSchema;
import org.apache.robux.segment.realtime.SegmentGenerationMetrics;
import org.apache.robux.segment.realtime.appenderator.Appenderator;
import org.apache.robux.segment.realtime.appenderator.BatchAppenderatorDriver;
import org.apache.robux.segment.realtime.appenderator.SegmentAllocator;
import org.apache.robux.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.robux.timeline.DataSegment;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Base class for parallel indexing perfect rollup worker partial segment generate tasks.
 */
abstract class PartialSegmentGenerateTask<T extends GeneratedPartitionsReport> extends PerfectRollupWorkerTask
{
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final String supervisorTaskId;
  private final IndexTaskInputRowIteratorBuilder inputRowIteratorBuilder;

  @MonotonicNonNull
  private RowIngestionMeters buildSegmentsMeters;

  @MonotonicNonNull
  private ParseExceptionHandler parseExceptionHandler;

  PartialSegmentGenerateTask(
      String id,
      String groupId,
      TaskResource taskResource,
      String supervisorTaskId,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      IndexTaskInputRowIteratorBuilder inputRowIteratorBuilder
  )
  {
    super(
        id,
        groupId,
        taskResource,
        ingestionSchema.getDataSchema(),
        ingestionSchema.getTuningConfig(),
        context,
        supervisorTaskId
    );

    Preconditions.checkArgument(
        !ingestionSchema.getDataSchema().getGranularitySpec().inputIntervals().isEmpty(),
        "Missing intervals in granularitySpec"
    );
    this.ingestionSchema = ingestionSchema;
    this.supervisorTaskId = supervisorTaskId;
    this.inputRowIteratorBuilder = inputRowIteratorBuilder;
  }

  @Override
  public final TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    InputSource inputSource = ingestionSchema.getIOConfig().getNonNullInputSource(toolbox);

    final ParallelIndexSupervisorTaskClient taskClient = toolbox.getSupervisorTaskClientProvider().build(
        supervisorTaskId,
        ingestionSchema.getTuningConfig().getChatHandlerTimeout(),
        ingestionSchema.getTuningConfig().getChatHandlerNumRetries()
    );

    final List<DataSegment> segments = generateSegments(
        toolbox,
        taskClient,
        inputSource,
        toolbox.getIndexingTmpDir()
    );

    TaskReport.ReportMap taskReport = getTaskCompletionReports(getNumSegmentsRead(inputSource));

    taskClient.report(createGeneratedPartitionsReport(toolbox, segments, taskReport));

    return TaskStatus.success(getId());
  }

  /**
   * @return {@link SegmentAllocator} suitable for the desired segment partitioning strategy.
   */
  abstract SegmentAllocatorForBatch createSegmentAllocator(
      TaskToolbox toolbox,
      ParallelIndexSupervisorTaskClient taskClient
  ) throws IOException;

  /**
   * @return {@link GeneratedPartitionsReport} suitable for the desired segment partitioning strategy.
   */
  abstract T createGeneratedPartitionsReport(
      TaskToolbox toolbox,
      List<DataSegment> segments,
      TaskReport.ReportMap taskReport
  );

  private Long getNumSegmentsRead(InputSource inputSource)
  {
    if (inputSource instanceof RobuxInputSource) {
      List<WindowedSegmentId> segments = ((RobuxInputSource) inputSource).getSegmentIds();
      if (segments != null) {
        return (long) segments.size();
      }
    }

    return null;
  }

  private List<DataSegment> generateSegments(
      final TaskToolbox toolbox,
      final ParallelIndexSupervisorTaskClient taskClient,
      final InputSource inputSource,
      final File tmpDir
  ) throws IOException, InterruptedException, ExecutionException, TimeoutException
  {
    final DataSchema dataSchema = ingestionSchema.getDataSchema();
    final SegmentGenerationMetrics segmentGenerationMetrics = new SegmentGenerationMetrics();
    buildSegmentsMeters = toolbox.getRowIngestionMetersFactory().createRowIngestionMeters();
    final TaskRealtimeMetricsMonitor metricsMonitor =
        TaskRealtimeMetricsMonitorBuilder.build(this, segmentGenerationMetrics, buildSegmentsMeters);
    toolbox.addMonitor(metricsMonitor);

    final ParallelIndexTuningConfig tuningConfig = ingestionSchema.getTuningConfig();
    final PartitionsSpec partitionsSpec = tuningConfig.getGivenOrDefaultPartitionsSpec();
    final long pushTimeout = tuningConfig.getPushTimeout();

    final SegmentAllocatorForBatch segmentAllocator = createSegmentAllocator(toolbox, taskClient);
    final SequenceNameFunction sequenceNameFunction = segmentAllocator.getSequenceNameFunction();

    parseExceptionHandler = new ParseExceptionHandler(
        buildSegmentsMeters,
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );
    final Appenderator appenderator = BatchAppenderators.newAppenderator(
        getId(),
        toolbox.getAppenderatorsManager(),
        segmentGenerationMetrics,
        toolbox,
        dataSchema,
        tuningConfig,
        new ShuffleDataSegmentPusher(supervisorTaskId, getId(), toolbox.getIntermediaryDataManager()),
        buildSegmentsMeters,
        parseExceptionHandler
    );
    boolean exceptionOccurred = false;
    try (final BatchAppenderatorDriver driver = BatchAppenderators.newDriver(appenderator, toolbox, segmentAllocator)) {
      driver.startJob();

      final Pair<SegmentsAndCommitMetadata, SegmentSchemaMapping> pushed = InputSourceProcessor.process(
          dataSchema,
          driver,
          partitionsSpec,
          inputSource,
          inputSource.needsFormat() ? ParallelIndexSupervisorTask.getInputFormat(ingestionSchema) : null,
          tmpDir,
          sequenceNameFunction,
          inputRowIteratorBuilder,
          buildSegmentsMeters,
          parseExceptionHandler,
          pushTimeout
      );
      return pushed.lhs.getSegments();
    }
    catch (Exception e) {
      exceptionOccurred = true;
      throw e;
    }
    finally {
      if (exceptionOccurred) {
        appenderator.closeNow();
      } else {
        appenderator.close();
      }
      toolbox.removeMonitor(metricsMonitor);
    }
  }

  /**
   * Generate an IngestionStatsAndErrorsTaskReport for the task.
   */
  private TaskReport.ReportMap getTaskCompletionReports(Long segmentsRead)
  {
    return buildIngestionStatsReport(
        IngestionState.COMPLETED,
        "",
        segmentsRead,
        null
    );
  }

  @Override
  protected Map<String, Object> getTaskCompletionUnparseableEvents()
  {
    List<ParseExceptionReport> parseExceptionMessages = Objects.requireNonNullElse(
        parseExceptionHandler.getSavedParseExceptionReports(),
        List.of()
    );

    return Map.of(RowIngestionMeters.BUILD_SEGMENTS, parseExceptionMessages);
  }

  @Override
  protected Map<String, Object> getTaskCompletionRowStats()
  {
    return Collections.singletonMap(
        RowIngestionMeters.BUILD_SEGMENTS,
        buildSegmentsMeters.getTotals()
    );
  }
}

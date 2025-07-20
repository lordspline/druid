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

package org.apache.robux.msq.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.msq.counters.ChannelCounters;
import org.apache.robux.msq.counters.CounterSnapshotsSerializer;
import org.apache.robux.msq.counters.CpuCounter;
import org.apache.robux.msq.counters.CpuCounters;
import org.apache.robux.msq.counters.NilQueryCounterSnapshot;
import org.apache.robux.msq.counters.SegmentGenerationProgressCounter;
import org.apache.robux.msq.counters.SuperSorterProgressTrackerCounter;
import org.apache.robux.msq.counters.WarningCounters;
import org.apache.robux.msq.indexing.MSQCompactionRunner;
import org.apache.robux.msq.indexing.MSQControllerTask;
import org.apache.robux.msq.indexing.MSQWorkerTask;
import org.apache.robux.msq.indexing.error.BroadcastTablesTooLargeFault;
import org.apache.robux.msq.indexing.error.CanceledFault;
import org.apache.robux.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.robux.msq.indexing.error.ColumnNameRestrictedFault;
import org.apache.robux.msq.indexing.error.ColumnTypeNotSupportedFault;
import org.apache.robux.msq.indexing.error.DurableStorageConfigurationFault;
import org.apache.robux.msq.indexing.error.InsertCannotAllocateSegmentFault;
import org.apache.robux.msq.indexing.error.InsertCannotBeEmptyFault;
import org.apache.robux.msq.indexing.error.InsertLockPreemptedFault;
import org.apache.robux.msq.indexing.error.InsertTimeNullFault;
import org.apache.robux.msq.indexing.error.InsertTimeOutOfBoundsFault;
import org.apache.robux.msq.indexing.error.InvalidFieldFault;
import org.apache.robux.msq.indexing.error.InvalidNullByteFault;
import org.apache.robux.msq.indexing.error.MSQFault;
import org.apache.robux.msq.indexing.error.NotEnoughMemoryFault;
import org.apache.robux.msq.indexing.error.NotEnoughTemporaryStorageFault;
import org.apache.robux.msq.indexing.error.QueryNotSupportedFault;
import org.apache.robux.msq.indexing.error.QueryRuntimeFault;
import org.apache.robux.msq.indexing.error.RowTooLargeFault;
import org.apache.robux.msq.indexing.error.TaskStartTimeoutFault;
import org.apache.robux.msq.indexing.error.TooManyAttemptsForJob;
import org.apache.robux.msq.indexing.error.TooManyAttemptsForWorker;
import org.apache.robux.msq.indexing.error.TooManyBucketsFault;
import org.apache.robux.msq.indexing.error.TooManyClusteredByColumnsFault;
import org.apache.robux.msq.indexing.error.TooManyColumnsFault;
import org.apache.robux.msq.indexing.error.TooManyInputFilesFault;
import org.apache.robux.msq.indexing.error.TooManyPartitionsFault;
import org.apache.robux.msq.indexing.error.TooManyRowsInAWindowFault;
import org.apache.robux.msq.indexing.error.TooManyRowsWithSameKeyFault;
import org.apache.robux.msq.indexing.error.TooManySegmentsInTimeChunkFault;
import org.apache.robux.msq.indexing.error.TooManyWarningsFault;
import org.apache.robux.msq.indexing.error.TooManyWorkersFault;
import org.apache.robux.msq.indexing.error.UnknownFault;
import org.apache.robux.msq.indexing.error.WorkerFailedFault;
import org.apache.robux.msq.indexing.error.WorkerRpcFailedFault;
import org.apache.robux.msq.indexing.processor.SegmentGeneratorStageProcessor;
import org.apache.robux.msq.indexing.report.MSQTaskReport;
import org.apache.robux.msq.input.NilInputSlice;
import org.apache.robux.msq.input.NilInputSource;
import org.apache.robux.msq.input.external.ExternalInputSlice;
import org.apache.robux.msq.input.external.ExternalInputSpec;
import org.apache.robux.msq.input.inline.InlineInputSlice;
import org.apache.robux.msq.input.inline.InlineInputSpec;
import org.apache.robux.msq.input.lookup.LookupInputSlice;
import org.apache.robux.msq.input.lookup.LookupInputSpec;
import org.apache.robux.msq.input.stage.StageInputSlice;
import org.apache.robux.msq.input.stage.StageInputSpec;
import org.apache.robux.msq.input.table.SegmentsInputSlice;
import org.apache.robux.msq.input.table.TableInputSpec;
import org.apache.robux.msq.kernel.NilExtraInfoHolder;
import org.apache.robux.msq.querykit.InputNumberDataSource;
import org.apache.robux.msq.querykit.RestrictedInputNumberDataSource;
import org.apache.robux.msq.querykit.WindowOperatorQueryStageProcessor;
import org.apache.robux.msq.querykit.common.OffsetLimitStageProcessor;
import org.apache.robux.msq.querykit.common.SortMergeJoinStageProcessor;
import org.apache.robux.msq.querykit.groupby.GroupByPostShuffleStageProcessor;
import org.apache.robux.msq.querykit.groupby.GroupByPreShuffleStageProcessor;
import org.apache.robux.msq.querykit.results.ExportResultsStageProcessor;
import org.apache.robux.msq.querykit.results.QueryResultStageProcessor;
import org.apache.robux.msq.querykit.scan.ScanQueryStageProcessor;
import org.apache.robux.msq.util.PassthroughAggregatorFactory;

import java.util.Collections;
import java.util.List;

/**
 * Module that adds {@link MSQControllerTask}, {@link MSQWorkerTask}, and dependencies.
 */
public class MSQIndexingModule implements RobuxModule
{
  static final String BASE_MSQ_KEY = "robux.msq";

  public static final List<Class<? extends MSQFault>> FAULT_CLASSES = ImmutableList.of(
      BroadcastTablesTooLargeFault.class,
      CanceledFault.class,
      CannotParseExternalDataFault.class,
      ColumnTypeNotSupportedFault.class,
      ColumnNameRestrictedFault.class,
      DurableStorageConfigurationFault.class,
      InsertCannotAllocateSegmentFault.class,
      InsertCannotBeEmptyFault.class,
      InsertLockPreemptedFault.class,
      InsertTimeNullFault.class,
      InsertTimeOutOfBoundsFault.class,
      InvalidFieldFault.class,
      InvalidNullByteFault.class,
      NotEnoughTemporaryStorageFault.class,
      NotEnoughMemoryFault.class,
      QueryNotSupportedFault.class,
      QueryRuntimeFault.class,
      RowTooLargeFault.class,
      TaskStartTimeoutFault.class,
      TooManyBucketsFault.class,
      TooManyClusteredByColumnsFault.class,
      TooManyColumnsFault.class,
      TooManyInputFilesFault.class,
      TooManyPartitionsFault.class,
      TooManyRowsInAWindowFault.class,
      TooManyRowsWithSameKeyFault.class,
      TooManySegmentsInTimeChunkFault.class,
      TooManyWarningsFault.class,
      TooManyWorkersFault.class,
      TooManyAttemptsForJob.class,
      UnknownFault.class,
      WorkerFailedFault.class,
      TooManyAttemptsForWorker.class,
      WorkerRpcFailedFault.class
  );

  @Override
  public List<? extends Module> getJacksonModules()
  {
    final SimpleModule module = new SimpleModule(getClass().getSimpleName());

    module.registerSubtypes(
        // Task classes
        MSQControllerTask.class,
        MSQWorkerTask.class,

        // FrameChannelWorkerFactory and FrameChannelWorkerFactoryExtraInfoHolder classes
        SegmentGeneratorStageProcessor.class,
        SegmentGeneratorStageProcessor.SegmentGeneratorExtraInfoHolder.class,
        ScanQueryStageProcessor.class,
        GroupByPreShuffleStageProcessor.class,
        GroupByPostShuffleStageProcessor.class,
        OffsetLimitStageProcessor.class,
        NilExtraInfoHolder.class,
        SortMergeJoinStageProcessor.class,
        QueryResultStageProcessor.class,
        WindowOperatorQueryStageProcessor.class,
        ExportResultsStageProcessor.class,

        // DataSource classes (note: ExternalDataSource is in MSQSqlModule)
        InputNumberDataSource.class,
        RestrictedInputNumberDataSource.class,

        // TaskReport classes
        MSQTaskReport.class,

        // QueryCounter.Snapshot classes
        ChannelCounters.Snapshot.class,
        SuperSorterProgressTrackerCounter.Snapshot.class,
        WarningCounters.Snapshot.class,
        SegmentGenerationProgressCounter.Snapshot.class,
        CpuCounters.Snapshot.class,
        CpuCounter.Snapshot.class,
        NilQueryCounterSnapshot.class,

        // InputSpec classes
        ExternalInputSpec.class,
        InlineInputSpec.class,
        LookupInputSpec.class,
        StageInputSpec.class,
        TableInputSpec.class,

        // InputSlice classes
        ExternalInputSlice.class,
        InlineInputSlice.class,
        LookupInputSlice.class,
        NilInputSlice.class,
        SegmentsInputSlice.class,
        StageInputSlice.class,

        // Other
        PassthroughAggregatorFactory.class,
        NilInputSource.class
    );

    module.registerSubtypes(new NamedType(MSQCompactionRunner.class, MSQCompactionRunner.TYPE));

    FAULT_CLASSES.forEach(module::registerSubtypes);
    module.addSerializer(new CounterSnapshotsSerializer());
    return Collections.singletonList(module);
  }

  @Override
  public void configure(Binder binder)
  {
  }
}

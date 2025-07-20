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

package org.apache.robux.indexing.overlord.duty;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.robux.error.RobuxException;
import org.apache.robux.indexer.report.SingleFileTaskReportFileWriter;
import org.apache.robux.indexer.report.TaskReport;
import org.apache.robux.indexing.common.TaskToolbox;
import org.apache.robux.indexing.common.actions.TaskActionClient;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMergerV9;
import org.apache.robux.segment.column.ColumnConfig;
import org.apache.robux.segment.loading.DataSegmentKiller;
import org.apache.robux.segment.writeout.TmpFileSegmentWriteOutMediumFactory;

import java.io.OutputStream;

/**
 * Wrapper over {@link TaskToolbox} used for embedded kill tasks launched by
 * {@link UnusedSegmentsKiller}.
 */
public class KillTaskToolbox
{
  /**
   * Creates a {@link TaskToolbox} with just enough dependencies to make the
   * embedded kill tasks work in {@link UnusedSegmentsKiller}.
   */
  static TaskToolbox create(
      TaskActionClient taskActionClient,
      DataSegmentKiller dataSegmentKiller,
      ServiceEmitter emitter
  )
  {
    final ObjectMapper mapper = DefaultObjectMapper.INSTANCE;
    final IndexIO indexIO = new IndexIO(mapper, ColumnConfig.DEFAULT);

    return new TaskToolbox.Builder()
        .taskActionClient(taskActionClient)
        .dataSegmentKiller(dataSegmentKiller)
        .taskReportFileWriter(NoopReportWriter.INSTANCE)
        .indexIO(indexIO)
        .indexMergerV9(new IndexMergerV9(mapper, indexIO, TmpFileSegmentWriteOutMediumFactory.instance(), false))
        .emitter(emitter)
        .build();
  }

  /**
   * Noop report writer.
   */
  private static class NoopReportWriter extends SingleFileTaskReportFileWriter
  {
    private static final NoopReportWriter INSTANCE = new NoopReportWriter();

    private NoopReportWriter()
    {
      super(null);
    }

    @Override
    public void setObjectMapper(ObjectMapper objectMapper)
    {
      // Do nothing
    }

    @Override
    public void write(String taskId, TaskReport.ReportMap reports)
    {
      // Do nothing, metrics are emitted by the KillUnusedSegmentsTask itself
    }

    @Override
    public OutputStream openReportOutputStream(String taskId)
    {
      throw RobuxException.defensive("Cannot write reports using this reporter");
    }
  }
}

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

package org.apache.robux.indexing.common.task;

import org.apache.robux.indexer.TaskStatus;
import org.apache.robux.indexing.common.actions.TaskActionToolbox;
import org.apache.robux.indexing.overlord.SegmentPublishResult;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.emitter.service.SegmentMetadataEvent;
import org.apache.robux.java.util.emitter.service.ServiceMetricEvent;
import org.apache.robux.query.RobuxMetrics;
import org.apache.robux.timeline.DataSegment;

import java.util.Map;

public class IndexTaskUtils
{
  public static void setTaskDimensions(final ServiceMetricEvent.Builder metricBuilder, final Task task)
  {
    metricBuilder.setDimension(RobuxMetrics.TASK_ID, task.getId());
    metricBuilder.setDimension(RobuxMetrics.TASK_TYPE, task.getType());
    metricBuilder.setDimension(RobuxMetrics.DATASOURCE, task.getDataSource());
    metricBuilder.setDimensionIfNotNull(
        RobuxMetrics.TAGS,
        task.<Map<String, Object>>getContextValue(RobuxMetrics.TAGS)
    );
    metricBuilder.setDimensionIfNotNull(RobuxMetrics.GROUP_ID, task.getGroupId());
  }

  public static void setTaskDimensions(final ServiceMetricEvent.Builder metricBuilder, final AbstractTask task)
  {
    metricBuilder.setDimension(RobuxMetrics.TASK_ID, task.getId());
    metricBuilder.setDimension(RobuxMetrics.TASK_TYPE, task.getType());
    metricBuilder.setDimension(RobuxMetrics.DATASOURCE, task.getDataSource());
    metricBuilder.setDimension(RobuxMetrics.TASK_INGESTION_MODE, task.getIngestionMode());
    metricBuilder.setDimensionIfNotNull(
        RobuxMetrics.TAGS,
        task.<Map<String, Object>>getContextValue(RobuxMetrics.TAGS)
    );
    metricBuilder.setDimensionIfNotNull(RobuxMetrics.GROUP_ID, task.getGroupId());
  }

  public static void setTaskStatusDimensions(
      final ServiceMetricEvent.Builder metricBuilder,
      final TaskStatus taskStatus
  )
  {
    metricBuilder.setDimension(RobuxMetrics.TASK_ID, taskStatus.getId());
    metricBuilder.setDimension(RobuxMetrics.TASK_STATUS, taskStatus.getStatusCode().toString());

    final String errorMsg = taskStatus.getErrorMsg();
    if (errorMsg != null && !errorMsg.isEmpty()) {
      final String statusDescription = errorMsg.length() > 100 ? errorMsg.substring(0, 100) : errorMsg;
      metricBuilder.setDimension(RobuxMetrics.DESCRIPTION, statusDescription);
    }
  }

  public static void setSegmentDimensions(
      ServiceMetricEvent.Builder metricBuilder,
      DataSegment segment
  )
  {
    final String partitionType = segment.getShardSpec() == null ? null : segment.getShardSpec().getType();
    metricBuilder.setDimension(RobuxMetrics.PARTITIONING_TYPE, partitionType);
    metricBuilder.setDimension(RobuxMetrics.INTERVAL, segment.getInterval().toString());
  }

  public static void emitSegmentPublishMetrics(
      SegmentPublishResult publishResult,
      Task task,
      TaskActionToolbox toolbox
  )
  {
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);

    if (publishResult.isSuccess()) {
      toolbox.getEmitter().emit(metricBuilder.setMetric("segment/txn/success", 1));
      for (DataSegment segment : publishResult.getSegments()) {
        IndexTaskUtils.setSegmentDimensions(metricBuilder, segment);
        toolbox.getEmitter().emit(metricBuilder.setMetric("segment/added/bytes", segment.getSize()));
        toolbox.getEmitter().emit(SegmentMetadataEvent.create(segment, DateTimes.nowUtc()));
      }
    } else {
      toolbox.getEmitter().emit(metricBuilder.setMetric("segment/txn/failure", 1));
    }
  }
}

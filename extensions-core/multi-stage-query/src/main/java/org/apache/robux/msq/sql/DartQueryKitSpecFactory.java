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

package org.apache.robux.msq.sql;

import com.google.inject.Inject;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.msq.dart.controller.DartControllerContext;
import org.apache.robux.msq.exec.QueryKitSpecFactory;
import org.apache.robux.msq.indexing.MSQTuningConfig;
import org.apache.robux.msq.querykit.QueryKit;
import org.apache.robux.msq.querykit.QueryKitSpec;
import org.apache.robux.msq.util.MultiStageQueryContext;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryContext;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.coordination.ServerType;

public class DartQueryKitSpecFactory implements QueryKitSpecFactory
{
  private final TimelineServerView serverView;

  @Inject
  public DartQueryKitSpecFactory(TimelineServerView serverView)
  {
    this.serverView = serverView;
  }

  @Override
  public QueryKitSpec makeQueryKitSpec(
      final QueryKit<Query<?>> queryKit,
      final String queryId,
      final MSQTuningConfig tuningConfig,
      final QueryContext queryContext)
  {
    return new QueryKitSpec(
        queryKit,
        queryId,
        getNumWorkers(),
        queryContext.getInt(
            DartControllerContext.CTX_MAX_NON_LEAF_WORKER_COUNT,
            DartControllerContext.DEFAULT_MAX_NON_LEAF_WORKER_COUNT
        ),
        MultiStageQueryContext.getTargetPartitionsPerWorkerWithDefault(
            queryContext,
            DartControllerContext.DEFAULT_TARGET_PARTITIONS_PER_WORKER
        )
    );
  }

  private int getNumWorkers()
  {
    int cnt = 0;
    for (RobuxServerMetadata s : serverView.getRobuxServerMetadatas()) {
      if (s.getType() == ServerType.HISTORICAL) {
        cnt++;
      }
    }

    // Even if all segments are realtime, launch at least one worker.
    return Math.max(1, cnt);
  }
}

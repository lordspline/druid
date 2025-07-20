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

package org.apache.robux.segment.metadata;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.robux.client.CoordinatorSegmentWatcherConfig;
import org.apache.robux.client.CoordinatorServerView;
import org.apache.robux.client.DirectRobuxClientFactory;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.SegmentLoadInfo;
import org.apache.robux.client.ServerInventoryView;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.query.DataSource;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.segment.realtime.appenderator.SegmentSchemas;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.VersionedIntervalTimeline;
import org.apache.robux.timeline.partition.ShardSpec;
import org.apache.robux.timeline.partition.SingleDimensionShardSpec;
import org.easymock.EasyMock;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

public class TestCoordinatorServerView extends CoordinatorServerView
{
  private static final RobuxServer DUMMY_SERVER = new RobuxServer(
      "dummy",
      "dummy",
      null,
      0,
      ServerType.HISTORICAL,
      "dummy",
      0
  );
  private static final RobuxServer DUMMY_SERVER_REALTIME = new RobuxServer(
      "dummy2",
      "dummy2",
      null,
      0,
      ServerType.INDEXER_EXECUTOR,
      "dummy",
      0
  );
  private static final RobuxServer DUMMY_BROKER = new RobuxServer(
      "dummy3",
      "dummy3",
      null,
      0,
      ServerType.BROKER,
      "dummy",
      0
  );

  private final Map<String, VersionedIntervalTimeline<String, SegmentLoadInfo>> timelines;
  private Map<Pair<DataSegment, ServerType>, Pair<RobuxServerMetadata, SegmentLoadInfo>> segmentInfo;
  private List<DataSegment> segments = new ArrayList<>();
  private List<DataSegment> realtimeSegments = new ArrayList<>();
  private List<DataSegment> brokerSegments = new ArrayList<>();
  private List<Pair<Executor, TimelineServerView.TimelineCallback>> timelineCallbackExecs = new ArrayList<>();

  public TestCoordinatorServerView(List<DataSegment> segments, List<DataSegment> realtimeSegments)
  {
    super(
        Mockito.mock(ServerInventoryView.class),
        Mockito.mock(CoordinatorSegmentWatcherConfig.class),
        Mockito.mock(ServiceEmitter.class),
        Mockito.mock(DirectRobuxClientFactory.class)
    );

    timelines = new HashMap<>();
    segmentInfo = new HashMap<>();

    for (DataSegment segment : segments) {
      addToTimeline(segment, DUMMY_SERVER);
    }

    for (DataSegment realtimeSegment : realtimeSegments) {
      addToTimeline(realtimeSegment, DUMMY_SERVER_REALTIME);
    }
  }

  private RobuxServer getServerForType(ServerType serverType)
  {
    switch (serverType) {
      case BROKER:
        return DUMMY_BROKER;
      case INDEXER_EXECUTOR:
        return DUMMY_SERVER_REALTIME;
      default:
        return DUMMY_SERVER;
    }
  }

  private void addToTimeline(DataSegment dataSegment, RobuxServer robuxServer)
  {
    if (robuxServer.getMetadata().getType() == ServerType.INDEXER_EXECUTOR) {
      realtimeSegments.add(dataSegment);
    } else if (robuxServer.getMetadata().getType() == ServerType.BROKER) {
      brokerSegments.add(dataSegment);
    } else {
      segments.add(dataSegment);
    }
    SegmentDescriptor segmentDescriptor = dataSegment.getId().toDescriptor();
    SegmentLoadInfo segmentLoadInfo = new SegmentLoadInfo(dataSegment);
    segmentLoadInfo.addServer(robuxServer.getMetadata());

    segmentInfo.put(Pair.of(dataSegment, robuxServer.getType()), Pair.of(robuxServer.getMetadata(), segmentLoadInfo));

    TableDataSource tableDataSource = new TableDataSource(dataSegment.getDataSource());
    timelines.computeIfAbsent(tableDataSource.getName(), value -> new VersionedIntervalTimeline<>(Comparator.naturalOrder()));
    VersionedIntervalTimeline<String, SegmentLoadInfo> timeline = timelines.get(tableDataSource.getName());
    final ShardSpec shardSpec = new SingleDimensionShardSpec("dimAll", null, null, 0, 1);
    timeline.add(dataSegment.getInterval(), segmentDescriptor.getVersion(), shardSpec.createChunk(segmentLoadInfo));
  }

  @Override
  public QueryRunner getQueryRunner(String serverName)
  {
    return EasyMock.mock(QueryRunner.class);
  }

  @Override
  public VersionedIntervalTimeline<String, SegmentLoadInfo> getTimeline(DataSource dataSource)
  {
    return timelines.get(Iterables.getOnlyElement(dataSource.getTableNames()));
  }

  @Override
  public void registerTimelineCallback(final Executor exec, final TimelineServerView.TimelineCallback callback)
  {
    for (DataSegment segment : segments) {
      exec.execute(() -> callback.segmentAdded(DUMMY_SERVER.getMetadata(), segment));
    }
    for (DataSegment segment : realtimeSegments) {
      exec.execute(() -> callback.segmentAdded(DUMMY_SERVER_REALTIME.getMetadata(), segment));
    }
    exec.execute(callback::timelineInitialized);
    timelineCallbackExecs.add(new Pair<>(exec, callback));
  }

  public void addSegment(DataSegment segment, ServerType serverType)
  {
    RobuxServer robuxServer = getServerForType(serverType);
    addToTimeline(segment, robuxServer);

    timelineCallbackExecs.forEach(
        execAndCallback -> execAndCallback.lhs.execute(() -> execAndCallback.rhs.segmentAdded(robuxServer.getMetadata(), segment))
    );
  }

  public void removeSegment(DataSegment segment, ServerType serverType)
  {
    RobuxServerMetadata robuxServerMetadata;
    if (serverType == ServerType.BROKER) {
      robuxServerMetadata = DUMMY_BROKER.getMetadata();
      brokerSegments.remove(segment);
    } else if (serverType == ServerType.INDEXER_EXECUTOR) {
      robuxServerMetadata = DUMMY_SERVER_REALTIME.getMetadata();
      realtimeSegments.remove(segment);
    } else {
      robuxServerMetadata = DUMMY_SERVER.getMetadata();
      segments.remove(segment);
    }

    Pair<DataSegment, ServerType> key = Pair.of(segment, serverType);
    Pair<RobuxServerMetadata, SegmentLoadInfo> info = segmentInfo.get(key);

    segmentInfo.remove(key);

    if (null != info) {
      timelines.get(segment.getDataSource()).remove(
          segment.getInterval(),
          "0",
          new SingleDimensionShardSpec("dimAll", null, null, 0, 1)
              .createChunk(info.rhs)
      );
    }

    timelineCallbackExecs.forEach(
        execAndCallback -> execAndCallback.lhs.execute(() -> {
          execAndCallback.rhs.serverSegmentRemoved(robuxServerMetadata, segment);

          // Fire segmentRemoved if all replicas have been removed.
          if (!segments.contains(segment) && !brokerSegments.contains(segment) && !realtimeSegments.remove(segment)) {
            execAndCallback.rhs.segmentRemoved(segment);
          }
        })
    );
  }

  public void addSegmentSchemas(SegmentSchemas segmentSchemas)
  {
    timelineCallbackExecs.forEach(
        execAndCallback -> execAndCallback.lhs.execute(() -> execAndCallback.rhs.segmentSchemasAnnounced(segmentSchemas))
    );
  }

  @Nullable
  @Override
  public List<RobuxServer> getInventory()
  {
    return Lists.newArrayList(DUMMY_SERVER, DUMMY_SERVER_REALTIME);
  }

  public List<DataSegment> getSegmentsOfServer(RobuxServer robuxServer)
  {
    if (robuxServer.getType() == ServerType.BROKER) {
      return Lists.newArrayList(brokerSegments);
    } else if (robuxServer.getType() == ServerType.INDEXER_EXECUTOR) {
      return Lists.newArrayList(realtimeSegments);
    } else {
      return Lists.newArrayList(segments);
    }
  }
}

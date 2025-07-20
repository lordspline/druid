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

package org.apache.robux.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.apache.robux.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.robux.client.selector.HistoricalFilter;
import org.apache.robux.client.selector.RandomServerSelectorStrategy;
import org.apache.robux.client.selector.ServerSelector;
import org.apache.robux.client.selector.TierSelectorStrategy;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QueryWatcher;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.TimelineLookup;
import org.apache.robux.timeline.VersionedIntervalTimeline;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

/**
 * A simple broker server view for testing which you can manually update the server view.
 */
public class SimpleServerView implements TimelineServerView
{
  private static final QueryWatcher NOOP_QUERY_WATCHER = (query, future) -> {};
  private final TierSelectorStrategy tierSelectorStrategy = new HighestPriorityTierSelectorStrategy(
      new RandomServerSelectorStrategy()
  );
  // server -> queryRunner
  private final Map<RobuxServer, QueryableRobuxServer> servers = new HashMap<>();
  // segmentId -> serverSelector
  private final Map<String, ServerSelector> selectors = new HashMap<>();
  // dataSource -> version -> serverSelector
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines = new HashMap<>();

  private final DirectRobuxClientFactory clientFactory;

  public SimpleServerView(
      QueryRunnerFactoryConglomerate conglomerate,
      ObjectMapper objectMapper,
      HttpClient httpClient
  )
  {
    this.clientFactory = new DirectRobuxClientFactory(
        new NoopServiceEmitter(),
        conglomerate,
        NOOP_QUERY_WATCHER,
        objectMapper,
        httpClient
    );
  }

  public void addServer(RobuxServer server, DataSegment dataSegment)
  {
    servers.put(server, new QueryableRobuxServer(server, clientFactory.makeDirectClient(server)));
    addSegmentToServer(server, dataSegment);
  }

  public void removeServer(RobuxServer server)
  {
    servers.remove(server);
  }

  public void unannounceSegmentFromServer(RobuxServer server, DataSegment segment)
  {
    final QueryableRobuxServer queryableRobuxServer = servers.get(server);
    if (queryableRobuxServer == null) {
      throw new ISE("Unknown server [%s]", server);
    }
    final ServerSelector selector = selectors.get(segment.getId().toString());
    if (selector == null) {
      throw new ISE("Unknown segment [%s]", segment.getId());
    }
    if (!selector.removeServer(queryableRobuxServer)) {
      throw new ISE("Failed to remove segment[%s] from server[%s]", segment.getId(), server);
    }
    final VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
    if (timeline == null) {
      throw new ISE("Unknown datasource [%s]", segment.getDataSource());
    }
    timeline.remove(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
  }

  private void addSegmentToServer(RobuxServer server, DataSegment segment)
  {
    final ServerSelector selector = selectors.computeIfAbsent(
        segment.getId().toString(),
        k -> new ServerSelector(segment, tierSelectorStrategy, HistoricalFilter.IDENTITY_FILTER)
    );
    selector.addServerAndUpdateSegment(servers.get(server), segment);
    // broker needs to skip tombstones in its timelines
    timelines.computeIfAbsent(segment.getDataSource(), k -> new VersionedIntervalTimeline<>(Ordering.natural(), true))
             .add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
  }

  @Override
  public Optional<? extends TimelineLookup<String, ServerSelector>> getTimeline(TableDataSource table)
  {
    return Optional.ofNullable(timelines.get(table.getName()));
  }

  @Override
  public List<ImmutableRobuxServer> getRobuxServers()
  {
    return Collections.emptyList();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> QueryRunner<T> getQueryRunner(RobuxServer server)
  {
    final QueryableRobuxServer queryableRobuxServer = Preconditions.checkNotNull(servers.get(server), "server");
    return (QueryRunner<T>) queryableRobuxServer.getQueryRunner();
  }

  @Override
  public void registerTimelineCallback(Executor exec, TimelineCallback callback)
  {
    // do nothing
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
    // do nothing
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    // do nothing
  }

  public static RobuxServer createServer(int nameSuiffix)
  {
    return new RobuxServer(
        "server_" + nameSuiffix,
        "127.0.0." + nameSuiffix,
        null,
        Long.MAX_VALUE,
        ServerType.HISTORICAL,
        "default",
        0
    );
  }
}

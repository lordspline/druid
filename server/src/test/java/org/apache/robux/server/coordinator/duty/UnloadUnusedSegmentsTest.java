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

package org.apache.robux.server.coordinator.duty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.ImmutableRobuxDataSource;
import org.apache.robux.client.ImmutableRobuxServer;
import org.apache.robux.client.ImmutableRobuxServerTests;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.metadata.MetadataRuleManager;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordinator.RobuxCluster;
import org.apache.robux.server.coordinator.RobuxCoordinator;
import org.apache.robux.server.coordinator.RobuxCoordinatorRuntimeParams;
import org.apache.robux.server.coordinator.ServerHolder;
import org.apache.robux.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.robux.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.robux.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.robux.server.coordinator.rules.ForeverLoadRule;
import org.apache.robux.server.coordinator.stats.CoordinatorRunStats;
import org.apache.robux.server.coordinator.stats.Stats;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class UnloadUnusedSegmentsTest
{
  private RobuxCoordinator coordinator;
  private ImmutableRobuxServer historicalServer;
  private ImmutableRobuxServer historicalServerTier2;
  private ImmutableRobuxServer brokerServer;
  private ImmutableRobuxServer indexerServer;
  private TestLoadQueuePeon historicalPeon;
  private TestLoadQueuePeon historicalTier2Peon;
  private TestLoadQueuePeon brokerPeon;
  private TestLoadQueuePeon indexerPeon;
  private DataSegment segment1;
  private DataSegment segment2;
  private List<DataSegment> segments;
  private List<DataSegment> segmentsForRealtime;
  private List<ImmutableRobuxDataSource> dataSources;
  private List<ImmutableRobuxDataSource> dataSourcesForRealtime;
  private final String broadcastDatasource = "broadcastDatasource";
  private MetadataRuleManager databaseRuleManager;
  private SegmentLoadQueueManager loadQueueManager;

  @Before
  public void setUp()
  {
    coordinator = EasyMock.createMock(RobuxCoordinator.class);
    historicalServer = EasyMock.createMock(ImmutableRobuxServer.class);
    historicalServerTier2 = EasyMock.createMock(ImmutableRobuxServer.class);
    brokerServer = EasyMock.createMock(ImmutableRobuxServer.class);
    indexerServer = EasyMock.createMock(ImmutableRobuxServer.class);
    databaseRuleManager = EasyMock.createMock(MetadataRuleManager.class);
    loadQueueManager = new SegmentLoadQueueManager(null, null);

    DateTime start1 = DateTimes.of("2012-01-01");
    DateTime start2 = DateTimes.of("2012-02-01");
    DateTime version = DateTimes.of("2012-05-01");
    segment1 = new DataSegment(
        "datasource1",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        11L
    );
    segment2 = new DataSegment(
        "datasource2",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    final DataSegment realtimeOnlySegment = new DataSegment(
        "datasource2",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    final DataSegment broadcastSegment = new DataSegment(
        broadcastDatasource,
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Collections.emptyMap(),
        Collections.emptyList(),
        Collections.emptyList(),
        NoneShardSpec.instance(),
        0,
        7L
    );

    segments = new ArrayList<>();
    segments.add(segment1);
    segments.add(segment2);
    segments.add(broadcastSegment);

    segmentsForRealtime = new ArrayList<>();
    segmentsForRealtime.add(realtimeOnlySegment);
    segmentsForRealtime.add(broadcastSegment);

    historicalPeon = new TestLoadQueuePeon();
    historicalTier2Peon = new TestLoadQueuePeon();
    brokerPeon = new TestLoadQueuePeon();
    indexerPeon = new TestLoadQueuePeon();

    final ImmutableRobuxDataSource dataSource1 = new ImmutableRobuxDataSource(
        "datasource1",
        Collections.emptyMap(),
        Collections.singleton(segment1)
    );
    final ImmutableRobuxDataSource dataSource2 = new ImmutableRobuxDataSource(
        "datasource2",
        Collections.emptyMap(),
        Collections.singleton(segment2)
    );

    final ImmutableRobuxDataSource broadcastDatasource = new ImmutableRobuxDataSource(
        "broadcastDatasource",
        Collections.emptyMap(),
        Collections.singleton(broadcastSegment)
    );

    dataSources = ImmutableList.of(dataSource1, dataSource2, broadcastDatasource);

    // This simulates a task that is ingesting to an existing non-broadcast datasource, with unpublished segments,
    // while also having a broadcast segment loaded.
    final ImmutableRobuxDataSource dataSource2ForRealtime = new ImmutableRobuxDataSource(
        "datasource2",
        Collections.emptyMap(),
        Collections.singleton(realtimeOnlySegment)
    );
    dataSourcesForRealtime = ImmutableList.of(dataSource2ForRealtime, broadcastDatasource);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(historicalServer);
    EasyMock.verify(historicalServerTier2);
    EasyMock.verify(brokerServer);
    EasyMock.verify(indexerServer);
    EasyMock.verify(databaseRuleManager);
  }

  @Test
  public void test_unloadUnusedSegmentsFromAllServers()
  {
    mockRobuxServer(
        historicalServer,
        ServerType.HISTORICAL,
        "historical",
        RobuxServer.DEFAULT_TIER,
        30L,
        100L,
        segments,
        dataSources
    );
    mockRobuxServer(
        historicalServerTier2,
        ServerType.HISTORICAL,
        "historicalTier2",
        "tier2",
        30L,
        100L,
        segments,
        dataSources
    );
    mockRobuxServer(
        brokerServer,
        ServerType.BROKER,
        "broker",
        RobuxServer.DEFAULT_TIER,
        30L,
        100L,
        segments,
        dataSources
    );
    mockRobuxServer(
        indexerServer,
        ServerType.INDEXER_EXECUTOR,
        "indexer",
        RobuxServer.DEFAULT_TIER,
        30L,
        100L,
        segmentsForRealtime,
        dataSourcesForRealtime
    );

    // Mock stuff that the coordinator needs
    mockCoordinator(coordinator);

    mockRuleManager(databaseRuleManager);

    // We keep datasource2 segments only, drop datasource1 and broadcastDatasource from all servers
    // realtimeSegment is intentionally missing from the set, to match how a realtime tasks's unpublished segments
    // will not appear in the coordinator's view of used segments.
    Set<DataSegment> usedSegments = ImmutableSet.of(segment2);

    RobuxCoordinatorRuntimeParams params = RobuxCoordinatorRuntimeParams
        .builder()
        .withRobuxCluster(
            RobuxCluster
                .builder()
                .addTier(
                    RobuxServer.DEFAULT_TIER,
                    new ServerHolder(historicalServer, historicalPeon, false)
                )
                .addTier(
                    "tier2",
                    new ServerHolder(historicalServerTier2, historicalTier2Peon, false)
                )
                .addBrokers(new ServerHolder(brokerServer, brokerPeon, false))
                .addRealtimes(new ServerHolder(indexerServer, indexerPeon, false))
                .build()
        )
        .withUsedSegments(usedSegments)
        .withBroadcastDatasources(Collections.singleton(broadcastDatasource))
        .build();

    params = new UnloadUnusedSegments(loadQueueManager, databaseRuleManager::getRulesWithDefault).run(params);
    CoordinatorRunStats stats = params.getCoordinatorStats();

    // We drop segment1 and broadcast1 from all servers, realtimeSegment is not dropped by the indexer
    Assert.assertEquals(2L, stats.getSegmentStat(Stats.Segments.UNNEEDED, RobuxServer.DEFAULT_TIER, segment1.getDataSource()));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.UNNEEDED, "tier2", segment1.getDataSource()));

    Assert.assertEquals(3L, stats.getSegmentStat(Stats.Segments.UNNEEDED, RobuxServer.DEFAULT_TIER, broadcastDatasource));
    Assert.assertEquals(1L, stats.getSegmentStat(Stats.Segments.UNNEEDED, "tier2", broadcastDatasource));
  }

  private static void mockRobuxServer(
      ImmutableRobuxServer robuxServer,
      ServerType serverType,
      String name,
      String tier,
      long currentSize,
      long maxSize,
      List<DataSegment> segments,
      List<ImmutableRobuxDataSource> dataSources
  )
  {
    EasyMock.expect(robuxServer.getName()).andReturn(name).anyTimes();
    EasyMock.expect(robuxServer.getTier()).andReturn(tier).anyTimes();
    EasyMock.expect(robuxServer.getCurrSize()).andReturn(currentSize).anyTimes();
    EasyMock.expect(robuxServer.getMaxSize()).andReturn(maxSize).anyTimes();
    ImmutableRobuxServerTests.expectSegments(robuxServer, segments);
    EasyMock.expect(robuxServer.getHost()).andReturn(name).anyTimes();
    EasyMock.expect(robuxServer.getType()).andReturn(serverType).anyTimes();
    EasyMock.expect(robuxServer.getDataSources()).andReturn(dataSources).anyTimes();
    if (!segments.isEmpty()) {
      segments.forEach(
          s -> EasyMock.expect(robuxServer.getSegment(s.getId())).andReturn(s).anyTimes()
      );
    }
    EasyMock.expect(robuxServer.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(robuxServer);
  }

  private static void mockCoordinator(RobuxCoordinator coordinator)
  {
    EasyMock.replay(coordinator);
  }

  private static void mockRuleManager(MetadataRuleManager metadataRuleManager)
  {
    EasyMock.expect(metadataRuleManager.getRulesWithDefault("datasource1")).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(
                    RobuxServer.DEFAULT_TIER, 1,
                    "tier2", 1
                ),
                null
            )
        )).anyTimes();

    EasyMock.expect(metadataRuleManager.getRulesWithDefault("datasource2")).andReturn(
        Collections.singletonList(
            new ForeverLoadRule(
                ImmutableMap.of(
                    RobuxServer.DEFAULT_TIER, 1,
                    "tier2", 1
                ),
                null
            )
        )).anyTimes();

    EasyMock.expect(metadataRuleManager.getRulesWithDefault("broadcastDatasource")).andReturn(
        Collections.singletonList(
            new ForeverBroadcastDistributionRule()
        )).anyTimes();

    EasyMock.replay(metadataRuleManager);
  }
}

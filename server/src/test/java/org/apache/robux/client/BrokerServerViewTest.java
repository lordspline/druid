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
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.robux.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.robux.client.selector.RandomServerSelectorStrategy;
import org.apache.robux.client.selector.ServerSelector;
import org.apache.robux.curator.CuratorTestBase;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.query.CloneQueryMode;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QueryWatcher;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.realtime.appenderator.SegmentSchemas;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordination.TestCoordinatorClient;
import org.apache.robux.server.initialization.ZkPathsConfig;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.TimelineLookup;
import org.apache.robux.timeline.TimelineObjectHolder;
import org.apache.robux.timeline.partition.NoneShardSpec;
import org.apache.robux.timeline.partition.PartitionHolder;
import org.apache.robux.timeline.partition.SingleElementPartitionChunk;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

public class BrokerServerViewTest extends CuratorTestBase
{
  private final ObjectMapper jsonMapper;
  private final ZkPathsConfig zkPathsConfig;

  private CountDownLatch segmentViewInitLatch;
  private CountDownLatch serverAddedLatch;
  private CountDownLatch segmentAddedLatch;
  private CountDownLatch segmentRemovedLatch;

  private BatchServerInventoryView baseView;
  private BrokerServerView brokerServerView;
  private BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig;

  public BrokerServerViewTest()
  {
    jsonMapper = TestHelper.makeJsonMapper();
    zkPathsConfig = new ZkPathsConfig();
    brokerViewOfCoordinatorConfig = new BrokerViewOfCoordinatorConfig(new TestCoordinatorClient());
  }

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    brokerViewOfCoordinatorConfig.start();
    curator.start();
    curator.blockUntilConnected();
  }

  @Test
  public void testSingleServerAddedRemovedSegment() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(1);

    setupViews();

    final RobuxServer robuxServer = setupHistoricalServer("default_tier", "localhost:1234", 0);
    final DataSegment segment = dataSegmentWithIntervalAndVersion("2014-10-20T00:00:00Z/P1D", "v1");
    final int partition = segment.getShardSpec().getPartitionNum();
    final Interval intervals = Intervals.of("2014-10-20T00:00:00Z/P1D");
    announceSegmentForServer(robuxServer, segment, zkPathsConfig, jsonMapper);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineLookup<String, ServerSelector> timeline = brokerServerView.getTimeline(
        new TableDataSource("test_broker_server_view")
    ).get();
    List<TimelineObjectHolder<String, ServerSelector>> serverLookupRes = timeline.lookup(intervals);
    Assert.assertEquals(1, serverLookupRes.size());

    TimelineObjectHolder<String, ServerSelector> actualTimelineObjectHolder = serverLookupRes.get(0);
    Assert.assertEquals(intervals, actualTimelineObjectHolder.getInterval());
    Assert.assertEquals("v1", actualTimelineObjectHolder.getVersion());

    PartitionHolder<ServerSelector> actualPartitionHolder = actualTimelineObjectHolder.getObject();
    Assert.assertTrue(actualPartitionHolder.isComplete());
    Assert.assertEquals(1, Iterables.size(actualPartitionHolder));

    ServerSelector selector = (actualPartitionHolder.iterator().next()).getObject();
    Assert.assertFalse(selector.isEmpty());
    Assert.assertEquals(segment, selector.getSegment());
    Assert.assertEquals(robuxServer, selector.pick(null, CloneQueryMode.EXCLUDECLONES).getServer());
    Assert.assertNotNull(timeline.findChunk(intervals, "v1", partition));

    unannounceSegmentForServer(robuxServer, segment, zkPathsConfig);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));

    Assert.assertEquals(
        0,
        timeline.lookup(intervals).size()
    );
    Assert.assertNull(timeline.findChunk(intervals, "v1", partition));
  }

  @Test
  public void testMultipleServerAddedRemovedSegment() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(5);

    // temporarily set latch count to 1
    segmentRemovedLatch = new CountDownLatch(1);

    setupViews();

    final List<RobuxServer> robuxServers = Lists.transform(
        ImmutableList.of("localhost:0", "localhost:1", "localhost:2", "localhost:3", "localhost:4"),
        hostname -> setupHistoricalServer("default_tier", hostname, 0)
    );

    final List<DataSegment> segments = Lists.transform(
        ImmutableList.of(
            Pair.of("2011-04-01/2011-04-03", "v1"),
            Pair.of("2011-04-03/2011-04-06", "v1"),
            Pair.of("2011-04-01/2011-04-09", "v2"),
            Pair.of("2011-04-06/2011-04-09", "v3"),
            Pair.of("2011-04-01/2011-04-02", "v3")
        ), input -> dataSegmentWithIntervalAndVersion(input.lhs, input.rhs)
    );

    for (int i = 0; i < 5; ++i) {
      announceSegmentForServer(robuxServers.get(i), segments.get(i), zkPathsConfig, jsonMapper);
    }
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineLookup timeline = brokerServerView.getTimeline(
        new TableDataSource("test_broker_server_view")
    ).get();
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "v3", robuxServers.get(4), segments.get(4)),
            createExpected("2011-04-02/2011-04-06", "v2", robuxServers.get(2), segments.get(2)),
            createExpected("2011-04-06/2011-04-09", "v3", robuxServers.get(3), segments.get(3))
        ),
        timeline.lookup(
            Intervals.of(
                "2011-04-01/2011-04-09"
            )
        )
    );

    // unannounce the segment created by dataSegmentWithIntervalAndVersion("2011-04-01/2011-04-09", "v2")
    unannounceSegmentForServer(robuxServers.get(2), segments.get(2), zkPathsConfig);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));

    // renew segmentRemovedLatch since we still have 4 segments to unannounce
    segmentRemovedLatch = new CountDownLatch(4);

    timeline = brokerServerView.getTimeline(
        new TableDataSource("test_broker_server_view")
    ).get();
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "v3", robuxServers.get(4), segments.get(4)),
            createExpected("2011-04-02/2011-04-03", "v1", robuxServers.get(0), segments.get(0)),
            createExpected("2011-04-03/2011-04-06", "v1", robuxServers.get(1), segments.get(1)),
            createExpected("2011-04-06/2011-04-09", "v3", robuxServers.get(3), segments.get(3))
        ),
        timeline.lookup(
            Intervals.of(
                "2011-04-01/2011-04-09"
            )
        )
    );

    // unannounce all the segments
    for (int i = 0; i < 5; ++i) {
      // skip the one that was previously unannounced
      if (i != 2) {
        unannounceSegmentForServer(robuxServers.get(i), segments.get(i), zkPathsConfig);
      }
    }
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));

    Assert.assertEquals(
        0,
        ((List<TimelineObjectHolder>) timeline.lookup(Intervals.of("2011-04-01/2011-04-09"))).size()
    );
  }

  @Test
  public void testMultipleServerAndBroker() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    serverAddedLatch = new CountDownLatch(6);
    segmentAddedLatch = new CountDownLatch(6);

    // temporarily set latch count to 1
    segmentRemovedLatch = new CountDownLatch(1);

    setupViews();

    final RobuxServer robuxBroker = new RobuxServer(
        "localhost:5",
        "localhost:5",
        null,
        10000000L,
        ServerType.BROKER,
        "default_tier",
        0
    );

    // Materialize this list so all servers are set up
    final List<RobuxServer> robuxServers =
        ImmutableList.copyOf(
            Lists.transform(
                ImmutableList.of("localhost:0", "localhost:1", "localhost:2", "localhost:3", "localhost:4"),
                hostname -> setupHistoricalServer("default_tier", hostname, 0)
            )
        );

    setupZNodeForServer(robuxBroker, zkPathsConfig, jsonMapper);

    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(serverAddedLatch));

    // check server metadatas
    Assert.assertEquals(
        robuxServers.stream().map(RobuxServer::getMetadata).collect(Collectors.toSet()),
        ImmutableSet.copyOf(brokerServerView.getRobuxServerMetadatas())
    );

    final List<DataSegment> segments = Lists.transform(
        ImmutableList.of(
            Pair.of("2011-04-01/2011-04-03", "v1"),
            Pair.of("2011-04-03/2011-04-06", "v1"),
            Pair.of("2011-04-01/2011-04-09", "v2"),
            Pair.of("2011-04-06/2011-04-09", "v3"),
            Pair.of("2011-04-01/2011-04-02", "v3")
        ),
        input -> dataSegmentWithIntervalAndVersion(input.lhs, input.rhs)
    );

    DataSegment brokerSegment = dataSegmentWithIntervalAndVersion("2011-04-01/2011-04-11", "v4");
    announceSegmentForServer(robuxBroker, brokerSegment, zkPathsConfig, jsonMapper);
    for (int i = 0; i < 5; ++i) {
      announceSegmentForServer(robuxServers.get(i), segments.get(i), zkPathsConfig, jsonMapper);
    }
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineLookup timeline = brokerServerView.getTimeline(
        new TableDataSource("test_broker_server_view")
    ).get();

    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "v3", robuxServers.get(4), segments.get(4)),
            createExpected("2011-04-02/2011-04-06", "v2", robuxServers.get(2), segments.get(2)),
            createExpected("2011-04-06/2011-04-09", "v3", robuxServers.get(3), segments.get(3))
        ),
        timeline.lookup(
            Intervals.of(
                "2011-04-01/2011-04-09"
            )
        )
    );

    // unannounce the broker segment should do nothing to announcements
    unannounceSegmentForServer(robuxBroker, brokerSegment, zkPathsConfig);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));

    // renew segmentRemovedLatch since we still have 5 segments to unannounce
    segmentRemovedLatch = new CountDownLatch(5);

    timeline = brokerServerView.getTimeline(
        new TableDataSource("test_broker_server_view")
    ).get();

    // expect same set of segments as before
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "v3", robuxServers.get(4), segments.get(4)),
            createExpected("2011-04-02/2011-04-06", "v2", robuxServers.get(2), segments.get(2)),
            createExpected("2011-04-06/2011-04-09", "v3", robuxServers.get(3), segments.get(3))
        ),
        timeline.lookup(
            Intervals.of(
                "2011-04-01/2011-04-09"
            )
        )
    );

    // unannounce all the segments
    for (int i = 0; i < 5; ++i) {
      unannounceSegmentForServer(robuxServers.get(i), segments.get(i), zkPathsConfig);
    }
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));
  }

  @Test
  public void testMultipleTiers() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(4);
    segmentRemovedLatch = new CountDownLatch(0);

    // Setup a Broker that watches only Tier 2
    final String tier1 = "tier1";
    final String tier2 = "tier2";
    setupViews(Sets.newHashSet(tier2), null, true);

    // Historical Tier 1 has segments 1 and 2, Tier 2 has segments 2 and 3
    final RobuxServer server11 = setupHistoricalServer(tier1, "localhost:1", 1);
    final RobuxServer server21 = setupHistoricalServer(tier2, "localhost:2", 1);

    final DataSegment segment1 = dataSegmentWithIntervalAndVersion("2020-01-01/P1D", "v1");
    announceSegmentForServer(server11, segment1, zkPathsConfig, jsonMapper);

    final DataSegment segment2 = dataSegmentWithIntervalAndVersion("2020-01-02/P1D", "v1");
    announceSegmentForServer(server11, segment2, zkPathsConfig, jsonMapper);
    announceSegmentForServer(server21, segment2, zkPathsConfig, jsonMapper);

    final DataSegment segment3 = dataSegmentWithIntervalAndVersion("2020-01-03/P1D", "v1");
    announceSegmentForServer(server21, segment3, zkPathsConfig, jsonMapper);

    // Wait for the segments to be added
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    // Get the timeline for the datasource
    TimelineLookup<String, ServerSelector> timeline = brokerServerView.getTimeline(
        new TableDataSource(segment1.getDataSource())
    ).get();

    // Verify that the timeline has no entry for the interval of segment 1
    Assert.assertTrue(timeline.lookup(segment1.getInterval()).isEmpty());

    // Verify that there is one entry for the interval of segment 2
    List<TimelineObjectHolder<String, ServerSelector>> timelineHolders =
        timeline.lookup(segment2.getInterval());
    Assert.assertEquals(1, timelineHolders.size());

    TimelineObjectHolder<String, ServerSelector> timelineHolder = timelineHolders.get(0);
    Assert.assertEquals(segment2.getInterval(), timelineHolder.getInterval());
    Assert.assertEquals(segment2.getVersion(), timelineHolder.getVersion());

    PartitionHolder<ServerSelector> partitionHolder = timelineHolder.getObject();
    Assert.assertTrue(partitionHolder.isComplete());
    Assert.assertEquals(1, Iterables.size(partitionHolder));

    ServerSelector selector = (partitionHolder.iterator().next()).getObject();
    Assert.assertFalse(selector.isEmpty());
    Assert.assertEquals(segment2, selector.getSegment());

    // Verify that the ServerSelector always picks Tier 1
    for (int i = 0; i < 5; ++i) {
      Assert.assertEquals(server21, selector.pick(null, CloneQueryMode.EXCLUDECLONES).getServer());
    }
    Assert.assertEquals(Collections.singletonList(server21.getMetadata()), selector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testRealtimeTasksNotWatched() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(4);
    segmentRemovedLatch = new CountDownLatch(0);

    // Setup a Broker that watches only Historicals
    setupViews(null, null, false);

    // Historical has segments 2 and 3, Realtime has segments 1 and 2
    final RobuxServer realtimeServer = setupRobuxServer(ServerType.INDEXER_EXECUTOR, null, "realtime:1", 1);
    final RobuxServer historicalServer = setupHistoricalServer("tier1", "historical:2", 1);

    final DataSegment segment1 = dataSegmentWithIntervalAndVersion("2020-01-01/P1D", "v1");
    announceSegmentForServer(realtimeServer, segment1, zkPathsConfig, jsonMapper);

    final DataSegment segment2 = dataSegmentWithIntervalAndVersion("2020-01-02/P1D", "v1");
    announceSegmentForServer(realtimeServer, segment2, zkPathsConfig, jsonMapper);
    announceSegmentForServer(historicalServer, segment2, zkPathsConfig, jsonMapper);

    final DataSegment segment3 = dataSegmentWithIntervalAndVersion("2020-01-03/P1D", "v1");
    announceSegmentForServer(historicalServer, segment3, zkPathsConfig, jsonMapper);

    // Wait for the segments to be added
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    // Get the timeline for the datasource
    TimelineLookup<String, ServerSelector> timeline = brokerServerView.getTimeline(
        new TableDataSource(segment1.getDataSource())
    ).get();

    // Verify that the timeline has no entry for the interval of segment 1
    Assert.assertTrue(timeline.lookup(segment1.getInterval()).isEmpty());

    // Verify that there is one entry for the interval of segment 2
    List<TimelineObjectHolder<String, ServerSelector>> timelineHolders =
        timeline.lookup(segment2.getInterval());
    Assert.assertEquals(1, timelineHolders.size());

    TimelineObjectHolder<String, ServerSelector> timelineHolder = timelineHolders.get(0);
    Assert.assertEquals(segment2.getInterval(), timelineHolder.getInterval());
    Assert.assertEquals(segment2.getVersion(), timelineHolder.getVersion());

    PartitionHolder<ServerSelector> partitionHolder = timelineHolder.getObject();
    Assert.assertTrue(partitionHolder.isComplete());
    Assert.assertEquals(1, Iterables.size(partitionHolder));

    ServerSelector selector = (partitionHolder.iterator().next()).getObject();
    Assert.assertFalse(selector.isEmpty());
    Assert.assertEquals(segment2, selector.getSegment());

    // Verify that the ServerSelector always picks the Historical server
    for (int i = 0; i < 5; ++i) {
      Assert.assertEquals(historicalServer, selector.pick(null, CloneQueryMode.EXCLUDECLONES).getServer());
    }
    Assert.assertEquals(Collections.singletonList(historicalServer.getMetadata()), selector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testIgnoredTiers() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(4);
    segmentRemovedLatch = new CountDownLatch(0);

    // Setup a Broker that does not watch Tier 1
    final String tier1 = "tier1";
    final String tier2 = "tier2";
    setupViews(null, Sets.newHashSet(tier1), false);

    // Historical Tier 1 has segments 1 and 2, Tier 2 has segments 2 and 3
    final RobuxServer server11 = setupHistoricalServer(tier1, "localhost:1", 1);
    final RobuxServer server21 = setupHistoricalServer(tier2, "localhost:2", 1);

    final DataSegment segment1 = dataSegmentWithIntervalAndVersion("2020-01-01/P1D", "v1");
    announceSegmentForServer(server11, segment1, zkPathsConfig, jsonMapper);

    final DataSegment segment2 = dataSegmentWithIntervalAndVersion("2020-01-02/P1D", "v1");
    announceSegmentForServer(server11, segment2, zkPathsConfig, jsonMapper);
    announceSegmentForServer(server21, segment2, zkPathsConfig, jsonMapper);

    final DataSegment segment3 = dataSegmentWithIntervalAndVersion("2020-01-03/P1D", "v1");
    announceSegmentForServer(server21, segment3, zkPathsConfig, jsonMapper);

    // Wait for the segments to be added
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    // Get the timeline for the datasource
    TimelineLookup<String, ServerSelector> timeline = brokerServerView.getTimeline(
        new TableDataSource(segment1.getDataSource())
    ).get();

    // Verify that the timeline has no entry for the interval of segment 1
    Assert.assertTrue(timeline.lookup(segment1.getInterval()).isEmpty());

    // Verify that there is one entry for the interval of segment 2
    List<TimelineObjectHolder<String, ServerSelector>> timelineHolders =
        timeline.lookup(segment2.getInterval());
    Assert.assertEquals(1, timelineHolders.size());

    TimelineObjectHolder<String, ServerSelector> timelineHolder = timelineHolders.get(0);
    Assert.assertEquals(segment2.getInterval(), timelineHolder.getInterval());
    Assert.assertEquals(segment2.getVersion(), timelineHolder.getVersion());

    PartitionHolder<ServerSelector> partitionHolder = timelineHolder.getObject();
    Assert.assertTrue(partitionHolder.isComplete());
    Assert.assertEquals(1, Iterables.size(partitionHolder));

    ServerSelector selector = (partitionHolder.iterator().next()).getObject();
    Assert.assertFalse(selector.isEmpty());
    Assert.assertEquals(segment2, selector.getSegment());

    // Verify that the ServerSelector always picks Tier 1
    for (int i = 0; i < 5; ++i) {
      Assert.assertEquals(server21, selector.pick(null, CloneQueryMode.EXCLUDECLONES).getServer());
    }
    Assert.assertEquals(Collections.singletonList(server21.getMetadata()), selector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test(expected = ISE.class)
  public void testInvalidWatchedTiersConfig() throws Exception
  {
    // Verify that specifying both ignoredTiers and watchedTiers fails startup
    final String tier1 = "tier1";
    final String tier2 = "tier2";
    setupViews(Sets.newHashSet(tier2), Sets.newHashSet(tier1), true);
  }

  @Test(expected = ISE.class)
  public void testEmptyWatchedTiersConfig() throws Exception
  {
    setupViews(Collections.emptySet(), null, true);
  }

  @Test(expected = ISE.class)
  public void testEmptyIgnoredTiersConfig() throws Exception
  {
    setupViews(null, Collections.emptySet(), true);
  }

  /**
   * Creates a RobuxServer of type HISTORICAL and sets up a ZNode for it.
   */
  private RobuxServer setupHistoricalServer(String tier, String name, int priority)
  {
    return setupRobuxServer(ServerType.HISTORICAL, tier, name, priority);
  }

  /**
   * Creates a RobuxServer of the specified type and sets up a ZNode for it.
   */
  private RobuxServer setupRobuxServer(ServerType serverType, String tier, String name, int priority)
  {
    final RobuxServer robuxServer = new RobuxServer(
        name,
        name,
        null,
        1000000,
        serverType,
        tier,
        priority
    );
    setupZNodeForServer(robuxServer, zkPathsConfig, jsonMapper);
    return robuxServer;
  }

  private Pair<Interval, Pair<String, Pair<RobuxServer, DataSegment>>> createExpected(
      String intervalStr,
      String version,
      RobuxServer robuxServer,
      DataSegment segment
  )
  {
    return Pair.of(Intervals.of(intervalStr), Pair.of(version, Pair.of(robuxServer, segment)));
  }

  private void assertValues(
      List<Pair<Interval, Pair<String, Pair<RobuxServer, DataSegment>>>> expected, List<TimelineObjectHolder> actual
  )
  {
    Assert.assertEquals(expected.size(), actual.size());

    for (int i = 0; i < expected.size(); ++i) {
      Pair<Interval, Pair<String, Pair<RobuxServer, DataSegment>>> expectedPair = expected.get(i);
      TimelineObjectHolder<String, ServerSelector> actualTimelineObjectHolder = actual.get(i);

      Assert.assertEquals(expectedPair.lhs, actualTimelineObjectHolder.getInterval());
      Assert.assertEquals(expectedPair.rhs.lhs, actualTimelineObjectHolder.getVersion());

      PartitionHolder<ServerSelector> actualPartitionHolder = actualTimelineObjectHolder.getObject();
      Assert.assertTrue(actualPartitionHolder.isComplete());
      Assert.assertEquals(1, Iterables.size(actualPartitionHolder));

      ServerSelector selector = ((SingleElementPartitionChunk<ServerSelector>) actualPartitionHolder.iterator()
                                                                                                    .next()).getObject();
      Assert.assertFalse(selector.isEmpty());
      Assert.assertEquals(expectedPair.rhs.rhs.lhs, selector.pick(null, CloneQueryMode.EXCLUDECLONES).getServer());
      Assert.assertEquals(expectedPair.rhs.rhs.rhs, selector.getSegment());
    }
  }

  private void setupViews() throws Exception
  {
    setupViews(null, null, true);
  }

  private void setupViews(Set<String> watchedTiers, Set<String> ignoredTiers, boolean watchRealtimeTasks)
      throws Exception
  {
    baseView = new BatchServerInventoryView(
        zkPathsConfig,
        curator,
        jsonMapper,
        Predicates.alwaysTrue(),
        "test"
    )
    {
      @Override
      public void registerServerCallback(Executor exec, ServerCallback callback)
      {
        super.registerServerCallback(
            exec,
            new ServerCallback() {
              @Override
              public CallbackAction serverAdded(RobuxServer server)
              {
                final CallbackAction res = callback.serverAdded(server);
                serverAddedLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction serverRemoved(RobuxServer server)
              {
                return callback.serverRemoved(server);
              }
            }
        );
      }

      @Override
      public void registerSegmentCallback(Executor exec, final SegmentCallback callback)
      {
        super.registerSegmentCallback(
            exec,
            new SegmentCallback()
            {
              @Override
              public CallbackAction segmentAdded(RobuxServerMetadata server, DataSegment segment)
              {
                CallbackAction res = callback.segmentAdded(server, segment);
                segmentAddedLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentRemoved(RobuxServerMetadata server, DataSegment segment)
              {
                CallbackAction res = callback.segmentRemoved(server, segment);
                segmentRemovedLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentViewInitialized()
              {
                CallbackAction res = callback.segmentViewInitialized();
                segmentViewInitLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
              {
                return CallbackAction.CONTINUE;
              }
            }
        );
      }
    };

    DirectRobuxClientFactory robuxClientFactory = new DirectRobuxClientFactory(
        new NoopServiceEmitter(),
        EasyMock.createMock(QueryRunnerFactoryConglomerate.class),
        EasyMock.createMock(QueryWatcher.class),
        getSmileMapper(),
        EasyMock.createMock(HttpClient.class)
    );

    brokerServerView = new BrokerServerView(
        robuxClientFactory,
        baseView,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        new NoopServiceEmitter(),
        new BrokerSegmentWatcherConfig()
        {
          @Override
          public Set<String> getWatchedTiers()
          {
            return watchedTiers;
          }

          @Override
          public boolean isWatchRealtimeTasks()
          {
            return watchRealtimeTasks;
          }

          @Override
          public Set<String> getIgnoredTiers()
          {
            return ignoredTiers;
          }
        },
        brokerViewOfCoordinatorConfig
    );

    baseView.start();
    brokerServerView.start();
  }

  private DataSegment dataSegmentWithIntervalAndVersion(String intervalStr, String version)
  {
    return DataSegment.builder()
                      .dataSource("test_broker_server_view")
                      .interval(Intervals.of(intervalStr))
                      .loadSpec(
                          ImmutableMap.of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version(version)
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(0)
                      .build();
  }

  public ObjectMapper getSmileMapper()
  {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    final ObjectMapper retVal = new DefaultObjectMapper(smileFactory, "broker");
    retVal.getFactory().setCodec(retVal);
    return retVal;
  }

  @After
  public void tearDown() throws Exception
  {
    baseView.stop();
    tearDownServerAndCurator();
  }
}

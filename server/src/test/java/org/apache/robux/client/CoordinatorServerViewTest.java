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
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.curator.utils.ZKPaths;
import org.apache.robux.curator.CuratorTestBase;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.query.TableDataSource;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.realtime.appenderator.SegmentSchemas;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.initialization.ZkPathsConfig;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.TimelineLookup;
import org.apache.robux.timeline.TimelineObjectHolder;
import org.apache.robux.timeline.partition.NoneShardSpec;
import org.apache.robux.timeline.partition.PartitionHolder;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

@RunWith(Parameterized.class)
public class CoordinatorServerViewTest extends CuratorTestBase
{
  private ObjectMapper jsonMapper;
  private ZkPathsConfig zkPathsConfig;
  private String inventoryPath;

  private CountDownLatch segmentViewInitLatch;
  private CountDownLatch segmentAddedLatch;
  private CountDownLatch segmentRemovedLatch;

  private CountDownLatch callbackSegmentViewInitLatch;
  private CountDownLatch callbackSegmentAddedLatch;
  private CountDownLatch callbackSegmentRemovedLatch;
  private CountDownLatch callbackServerSegmentRemovedLatch;

  private BatchServerInventoryView baseView;
  private CoordinatorServerView coordinatorServerView;
  private ExecutorService callbackExec;

  private boolean setRobuxClientFactory;

  @Parameterized.Parameters
  public static Object[] data()
  {
    return new Object[]{true, false};
  }

  public CoordinatorServerViewTest(boolean setRobuxClientFactory)
  {
    this.setRobuxClientFactory = setRobuxClientFactory;
  }

  @Before
  public void setUp() throws Exception
  {
    jsonMapper = TestHelper.makeJsonMapper();
    zkPathsConfig = new ZkPathsConfig();
    inventoryPath = zkPathsConfig.getLiveSegmentsPath();
    callbackExec = Execs.singleThreaded("CoordinatorServerViewTest-%s");

    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
  }

  @Test
  public void testSingleServerAddedRemovedSegment() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(1);
    callbackSegmentViewInitLatch = new CountDownLatch(1);
    callbackSegmentAddedLatch = new CountDownLatch(1);
    callbackServerSegmentRemovedLatch = new CountDownLatch(1);
    callbackSegmentRemovedLatch = new CountDownLatch(1);

    setupViews(setRobuxClientFactory);

    final RobuxServer robuxServer = new RobuxServer(
        "localhost:1234",
        "localhost:1234",
        null,
        10000000L,
        ServerType.HISTORICAL,
        "default_tier",
        0
    );

    setupZNodeForServer(robuxServer, zkPathsConfig, jsonMapper);

    final DataSegment segment = dataSegmentWithIntervalAndVersion("2014-10-20T00:00:00Z/P1D", "v1");
    final int partition = segment.getShardSpec().getPartitionNum();
    final Interval intervals = Intervals.of("2014-10-20T00:00:00Z/P1D");
    announceSegmentForServer(robuxServer, segment, zkPathsConfig, jsonMapper);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackSegmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackSegmentAddedLatch));

    if (setRobuxClientFactory) {
      Assert.assertNotNull(coordinatorServerView.getQueryRunner(robuxServer.getName()));
    } else {
      Assert.assertNull(coordinatorServerView.getQueryRunner(robuxServer.getName()));
    }

    TimelineLookup timeline = coordinatorServerView.getTimeline(new TableDataSource("test_overlord_server_view"));
    List<TimelineObjectHolder> serverLookupRes = (List<TimelineObjectHolder>) timeline.lookup(
        intervals
    );
    Assert.assertEquals(1, serverLookupRes.size());

    TimelineObjectHolder<String, SegmentLoadInfo> actualTimelineObjectHolder = serverLookupRes.get(0);
    Assert.assertEquals(intervals, actualTimelineObjectHolder.getInterval());
    Assert.assertEquals("v1", actualTimelineObjectHolder.getVersion());

    PartitionHolder<SegmentLoadInfo> actualPartitionHolder = actualTimelineObjectHolder.getObject();
    Assert.assertTrue(actualPartitionHolder.isComplete());
    Assert.assertEquals(1, Iterables.size(actualPartitionHolder));

    SegmentLoadInfo segmentLoadInfo = actualPartitionHolder.iterator().next().getObject();
    Assert.assertFalse(segmentLoadInfo.isEmpty());
    Assert.assertEquals(
        robuxServer.getMetadata(),
        Iterables.getOnlyElement(segmentLoadInfo.toImmutableSegmentLoadInfo().getServers())
    );
    Assert.assertNotNull(timeline.findChunk(intervals, "v1", partition));

    unannounceSegmentForServer(robuxServer, segment);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackServerSegmentRemovedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackSegmentRemovedLatch));

    Assert.assertEquals(
        0,
        ((List<TimelineObjectHolder>) timeline.lookup(Intervals.of("2014-10-20T00:00:00Z/P1D"))).size()
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

    callbackSegmentViewInitLatch = new CountDownLatch(1);
    callbackSegmentAddedLatch = new CountDownLatch(5);
    callbackServerSegmentRemovedLatch = new CountDownLatch(1);
    callbackSegmentRemovedLatch = new CountDownLatch(1);

    setupViews(setRobuxClientFactory);

    final List<RobuxServer> robuxServers = Lists.transform(
        ImmutableList.of("localhost:0", "localhost:1", "localhost:2", "localhost:3", "localhost:4"),
        new Function<>()
        {
          @Override
          public RobuxServer apply(String input)
          {
            return new RobuxServer(
                input,
                input,
                null,
                10000000L,
                ServerType.HISTORICAL,
                "default_tier",
                0
            );
          }
        }
    );

    for (RobuxServer robuxServer : robuxServers) {
      setupZNodeForServer(robuxServer, zkPathsConfig, jsonMapper);
    }

    final List<DataSegment> segments = Lists.transform(
        ImmutableList.of(
            Pair.of("2011-04-01/2011-04-03", "v1"),
            Pair.of("2011-04-03/2011-04-06", "v1"),
            Pair.of("2011-04-01/2011-04-09", "v2"),
            Pair.of("2011-04-06/2011-04-09", "v3"),
            Pair.of("2011-04-01/2011-04-02", "v3")
        ), new Function<>()
        {
          @Override
          public DataSegment apply(Pair<String, String> input)
          {
            return dataSegmentWithIntervalAndVersion(input.lhs, input.rhs);
          }
        }
    );

    for (int i = 0; i < 5; ++i) {
      announceSegmentForServer(robuxServers.get(i), segments.get(i), zkPathsConfig, jsonMapper);
    }
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackSegmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackSegmentAddedLatch));

    for (int i = 0; i < 5; ++i) {
      if (setRobuxClientFactory) {
        Assert.assertNotNull(coordinatorServerView.getQueryRunner(robuxServers.get(i).getName()));
      } else {
        Assert.assertNull(coordinatorServerView.getQueryRunner(robuxServers.get(i).getName()));
      }
    }

    TimelineLookup timeline = coordinatorServerView.getTimeline(new TableDataSource("test_overlord_server_view"));
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "v3", robuxServers.get(4), segments.get(4)),
            createExpected("2011-04-02/2011-04-06", "v2", robuxServers.get(2), segments.get(2)),
            createExpected("2011-04-06/2011-04-09", "v3", robuxServers.get(3), segments.get(3))
        ),
        (List<TimelineObjectHolder>) timeline.lookup(
            Intervals.of(
                "2011-04-01/2011-04-09"
            )
        )
    );

    // unannounce the segment created by dataSegmentWithIntervalAndVersion("2011-04-01/2011-04-09", "v2")
    unannounceSegmentForServer(robuxServers.get(2), segments.get(2));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackSegmentRemovedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackServerSegmentRemovedLatch));

    // renew segmentRemovedLatch since we still have 4 segments to unannounce
    segmentRemovedLatch = new CountDownLatch(4);
    callbackServerSegmentRemovedLatch = new CountDownLatch(4);
    callbackSegmentRemovedLatch = new CountDownLatch(4);

    timeline = coordinatorServerView.getTimeline(new TableDataSource("test_overlord_server_view"));
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "v3", robuxServers.get(4), segments.get(4)),
            createExpected("2011-04-02/2011-04-03", "v1", robuxServers.get(0), segments.get(0)),
            createExpected("2011-04-03/2011-04-06", "v1", robuxServers.get(1), segments.get(1)),
            createExpected("2011-04-06/2011-04-09", "v3", robuxServers.get(3), segments.get(3))
        ),
        (List<TimelineObjectHolder>) timeline.lookup(Intervals.of("2011-04-01/2011-04-09"))
    );

    // unannounce all the segments
    for (int i = 0; i < 5; ++i) {
      // skip the one that was previously unannounced
      if (i != 2) {
        unannounceSegmentForServer(robuxServers.get(i), segments.get(i));
      }
    }
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackSegmentRemovedLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackServerSegmentRemovedLatch));

    Assert.assertEquals(
        0,
        ((List<TimelineObjectHolder>) timeline.lookup(Intervals.of("2011-04-01/2011-04-09"))).size()
    );
  }

  private void unannounceSegmentForServer(RobuxServer robuxServer, DataSegment segment) throws Exception
  {
    curator
        .delete()
        .guaranteed()
        .forPath(ZKPaths.makePath(inventoryPath, robuxServer.getHost(), segment.getId().toString()));
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
      TimelineObjectHolder<String, SegmentLoadInfo> actualTimelineObjectHolder = actual.get(i);

      Assert.assertEquals(expectedPair.lhs, actualTimelineObjectHolder.getInterval());
      Assert.assertEquals(expectedPair.rhs.lhs, actualTimelineObjectHolder.getVersion());

      PartitionHolder<SegmentLoadInfo> actualPartitionHolder = actualTimelineObjectHolder.getObject();
      Assert.assertTrue(actualPartitionHolder.isComplete());
      Assert.assertEquals(1, Iterables.size(actualPartitionHolder));

      SegmentLoadInfo segmentLoadInfo = actualPartitionHolder.iterator().next().getObject();
      Assert.assertFalse(segmentLoadInfo.isEmpty());
      Assert.assertEquals(expectedPair.rhs.rhs.lhs.getMetadata(),
                          Iterables.getOnlyElement(segmentLoadInfo.toImmutableSegmentLoadInfo().getServers()));
      Assert.assertEquals(expectedPair.rhs.rhs.lhs.getMetadata(), segmentLoadInfo.pickOne());
    }
  }

  private void setupViews(boolean setRobuxClientFactory) throws Exception
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

    DirectRobuxClientFactory robuxClientFactory = null;

    if (setRobuxClientFactory) {
      robuxClientFactory = EasyMock.createMock(DirectRobuxClientFactory.class);
      DirectRobuxClient directRobuxClient = EasyMock.mock(DirectRobuxClient.class);
      EasyMock.expect(robuxClientFactory.makeDirectClient(EasyMock.anyObject(RobuxServer.class)))
              .andReturn(directRobuxClient)
              .anyTimes();

      EasyMock.replay(robuxClientFactory);
    }

    coordinatorServerView = new CoordinatorServerView(
        baseView,
        new CoordinatorSegmentWatcherConfig(),
        new NoopServiceEmitter(),
        robuxClientFactory
    );

    baseView.start();
    initServerViewTimelineCallback(coordinatorServerView);
    coordinatorServerView.start();
  }

  private void initServerViewTimelineCallback(final CoordinatorServerView serverView)
  {
    serverView.registerTimelineCallback(
        callbackExec,
        new TimelineServerView.TimelineCallback()
        {
          @Override
          public ServerView.CallbackAction timelineInitialized()
          {
            callbackSegmentViewInitLatch.countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentAdded(final RobuxServerMetadata server, final DataSegment segment)
          {
            callbackSegmentAddedLatch.countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DataSegment segment)
          {
            callbackSegmentRemovedLatch.countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction serverSegmentRemoved(
              final RobuxServerMetadata server,
              final DataSegment segment
          )
          {
            callbackServerSegmentRemovedLatch.countDown();
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
          {
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  private DataSegment dataSegmentWithIntervalAndVersion(String intervalStr, String version)
  {
    return DataSegment.builder()
                      .dataSource("test_overlord_server_view")
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

  @After
  public void tearDown() throws Exception
  {
    baseView.stop();
    tearDownServerAndCurator();
  }
}

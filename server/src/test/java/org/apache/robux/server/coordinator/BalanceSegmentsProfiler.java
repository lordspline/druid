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

package org.apache.robux.server.coordinator;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.client.ImmutableRobuxServer;
import org.apache.robux.client.ImmutableRobuxServerTests;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.emitter.EmittingLogger;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.metadata.MetadataRuleManager;
import org.apache.robux.server.coordinator.duty.BalanceSegments;
import org.apache.robux.server.coordinator.duty.RunRules;
import org.apache.robux.server.coordinator.loading.LoadQueuePeon;
import org.apache.robux.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.robux.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.robux.server.coordinator.rules.PeriodLoadRule;
import org.apache.robux.server.coordinator.rules.Rule;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * TODO convert benchmarks to JMH
 */
public class BalanceSegmentsProfiler
{
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private SegmentLoadQueueManager loadQueueManager;
  private ImmutableRobuxServer robuxServer1;
  private ImmutableRobuxServer robuxServer2;
  List<DataSegment> segments = new ArrayList<>();
  ServiceEmitter emitter;
  MetadataRuleManager manager;
  PeriodLoadRule loadRule = new PeriodLoadRule(new Period("P5000Y"), null, ImmutableMap.of("normal", 3), null);
  List<Rule> rules = ImmutableList.of(loadRule);

  @Before
  public void setUp()
  {
    loadQueueManager = new SegmentLoadQueueManager(null, null);
    robuxServer1 = EasyMock.createMock(ImmutableRobuxServer.class);
    robuxServer2 = EasyMock.createMock(ImmutableRobuxServer.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    manager = EasyMock.createMock(MetadataRuleManager.class);
  }

  public void bigProfiler()
  {
    Stopwatch watch = Stopwatch.createUnstarted();
    int numSegments = 55000;
    int numServers = 50;
    EasyMock.expect(manager.getAllRules()).andReturn(ImmutableMap.of("test", rules)).anyTimes();
    EasyMock.expect(manager.getRules(EasyMock.anyObject())).andReturn(rules).anyTimes();
    EasyMock.expect(manager.getRulesWithDefault(EasyMock.anyObject())).andReturn(rules).anyTimes();
    EasyMock.replay(manager);

    List<ServerHolder> serverHolderList = new ArrayList<>();
    List<DataSegment> segments = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      segments.add(
          new DataSegment(
              "datasource" + i,
              new Interval(DateTimes.of("2012-01-01"), (DateTimes.of("2012-01-01")).plusHours(1)),
              (DateTimes.of("2012-03-01")).toString(),
              new HashMap<>(),
              new ArrayList<>(),
              new ArrayList<>(),
              NoneShardSpec.instance(),
              0,
              4L
          )
      );
    }

    for (int i = 0; i < numServers; i++) {
      ImmutableRobuxServer server = EasyMock.createMock(ImmutableRobuxServer.class);
      EasyMock.expect(server.getMetadata()).andReturn(null).anyTimes();
      EasyMock.expect(server.getCurrSize()).andReturn(30L).atLeastOnce();
      EasyMock.expect(server.getMaxSize()).andReturn(100L).atLeastOnce();
      EasyMock.expect(server.getTier()).andReturn("normal").anyTimes();
      EasyMock.expect(server.getName()).andReturn(Integer.toString(i)).atLeastOnce();
      EasyMock.expect(server.getHost()).andReturn(Integer.toString(i)).anyTimes();
      if (i == 0) {
        ImmutableRobuxServerTests.expectSegments(server, segments);
      } else {
        ImmutableRobuxServerTests.expectSegments(server, Collections.emptyList());
      }
      EasyMock.expect(server.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
      EasyMock.replay(server);

      LoadQueuePeon peon = new TestLoadQueuePeon();
      serverHolderList.add(new ServerHolder(server, peon));
    }

    RobuxCluster robuxCluster = RobuxCluster
        .builder()
        .addTier("normal", serverHolderList.toArray(new ServerHolder[0]))
        .build();
    RobuxCoordinatorRuntimeParams params = RobuxCoordinatorRuntimeParams
        .builder()
        .withRobuxCluster(robuxCluster)
        .withUsedSegments(segments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE)
                .withReplicantLifetime(500)
                .withReplicationThrottleLimit(5)
                .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .build();

    BalanceSegments tester = new BalanceSegments(Duration.standardMinutes(1));
    RunRules runner = new RunRules((ds, set) -> set.size(), manager::getRulesWithDefault);
    watch.start();
    RobuxCoordinatorRuntimeParams balanceParams = tester.run(params);
    RobuxCoordinatorRuntimeParams assignParams = runner.run(params);
    System.out.println(watch.stop());
  }


  public void profileRun()
  {
    Stopwatch watch = Stopwatch.createUnstarted();
    TestLoadQueuePeon fromPeon = new TestLoadQueuePeon();
    TestLoadQueuePeon toPeon = new TestLoadQueuePeon();

    EasyMock.expect(robuxServer1.getName()).andReturn("from").atLeastOnce();
    EasyMock.expect(robuxServer1.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(robuxServer1.getMaxSize()).andReturn(100L).atLeastOnce();
    ImmutableRobuxServerTests.expectSegments(robuxServer1, segments);
    EasyMock.expect(robuxServer1.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(robuxServer1);

    EasyMock.expect(robuxServer2.getName()).andReturn("to").atLeastOnce();
    EasyMock.expect(robuxServer2.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(robuxServer2.getCurrSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(robuxServer2.getMaxSize()).andReturn(100L).atLeastOnce();
    ImmutableRobuxServerTests.expectSegments(robuxServer2, Collections.emptyList());
    EasyMock.expect(robuxServer2.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(robuxServer2);

    RobuxCoordinatorRuntimeParams params = RobuxCoordinatorRuntimeParams
        .builder()
        .withRobuxCluster(
            RobuxCluster
                .builder()
                .addTier(
                    "normal",
                    new ServerHolder(robuxServer1, fromPeon),
                    new ServerHolder(robuxServer2, toPeon)
                )
                .build()
        )
        .withUsedSegments(segments)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE).build())
        .withSegmentAssignerUsing(loadQueueManager)
        .build();
    BalanceSegments tester = new BalanceSegments(Duration.standardMinutes(1));
    watch.start();
    RobuxCoordinatorRuntimeParams balanceParams = tester.run(params);
    System.out.println(watch.stop());
  }

}

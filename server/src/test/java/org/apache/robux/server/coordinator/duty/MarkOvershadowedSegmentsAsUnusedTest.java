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

import com.google.common.collect.Sets;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.ImmutableRobuxServer;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordinator.CoordinatorDynamicConfig;
import org.apache.robux.server.coordinator.RobuxCluster;
import org.apache.robux.server.coordinator.RobuxCoordinatorRuntimeParams;
import org.apache.robux.server.coordinator.ServerHolder;
import org.apache.robux.server.coordinator.balancer.RandomBalancerStrategy;
import org.apache.robux.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.robux.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.robux.server.coordinator.simulate.TestSegmentsMetadataManager;
import org.apache.robux.server.coordinator.stats.CoordinatorRunStats;
import org.apache.robux.server.coordinator.stats.Dimension;
import org.apache.robux.server.coordinator.stats.RowKey;
import org.apache.robux.server.coordinator.stats.Stats;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Set;

@RunWith(JUnitParamsRunner.class)
public class MarkOvershadowedSegmentsAsUnusedTest
{
  private final DateTime start = DateTimes.of("2012-01-01");

  private final DataSegment segmentV0 = DataSegment.builder().dataSource("test")
                                                   .interval(new Interval(start, start.plusHours(1)))
                                                   .version("0")
                                                   .size(0)
                                                   .build();
  private final DataSegment segmentV1 = segmentV0.withVersion("1");
  private final DataSegment segmentV2 = segmentV0.withVersion("2");

  private TestSegmentsMetadataManager segmentsMetadataManager;

  @Before
  public void setup()
  {
    segmentsMetadataManager = new TestSegmentsMetadataManager();
  }

  @Test
  @Parameters({"historical", "broker"})
  public void testRun(String serverType)
  {
    segmentsMetadataManager.addSegment(segmentV0);
    segmentsMetadataManager.addSegment(segmentV1);
    segmentsMetadataManager.addSegment(segmentV2);

    final ImmutableRobuxServer robuxServer =
        new RobuxServer("", "", "", 0L, ServerType.fromString(serverType), "", 0)
            .addDataSegment(segmentV1)
            .addDataSegment(segmentV2)
            .toImmutableRobuxServer();

    RobuxCluster robuxCluster = RobuxCluster
        .builder()
        .add(new ServerHolder(robuxServer, new TestLoadQueuePeon()))
        .build();

    RobuxCoordinatorRuntimeParams params = RobuxCoordinatorRuntimeParams
        .builder()
        .withDataSourcesSnapshot(
            segmentsMetadataManager.getRecentDataSourcesSnapshot()
        )
        .withRobuxCluster(robuxCluster)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder().withMarkSegmentAsUnusedDelayMillis(0).build()
        )
        .withBalancerStrategy(new RandomBalancerStrategy())
        .withSegmentAssignerUsing(new SegmentLoadQueueManager(null, null))
        .build();

    SegmentTimeline timeline = segmentsMetadataManager.getRecentDataSourcesSnapshot()
                                                      .getUsedSegmentsTimelinesPerDataSource()
                                                      .get("test");

    // Verify that the segments V0 and V1 are overshadowed
    Assert.assertTrue(timeline.isOvershadowed(segmentV0));
    Assert.assertTrue(timeline.isOvershadowed(segmentV1));

    // Run the duty and verify that the overshadowed segments are marked unused
    params = new MarkOvershadowedSegmentsAsUnused(
        (ds, segmentIds) -> segmentsMetadataManager.markSegmentsAsUnused(segmentIds)
    ).run(params);

    Set<DataSegment> updatedUsedSegments = Sets.newHashSet(
        segmentsMetadataManager.getRecentDataSourcesSnapshot().iterateAllUsedSegmentsInSnapshot()
    );
    Assert.assertEquals(1, updatedUsedSegments.size());
    Assert.assertTrue(updatedUsedSegments.contains(segmentV2));

    CoordinatorRunStats runStats = params.getCoordinatorStats();
    Assert.assertEquals(
        2L,
        runStats.get(Stats.Segments.OVERSHADOWED, RowKey.of(Dimension.DATASOURCE, "test"))
    );
  }
}

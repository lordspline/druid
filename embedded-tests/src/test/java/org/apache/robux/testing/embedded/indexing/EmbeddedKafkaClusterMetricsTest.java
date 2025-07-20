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

package org.apache.robux.testing.embedded.indexing;

import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.data.input.impl.DimensionsSpec;
import org.apache.robux.data.input.impl.JsonInputFormat;
import org.apache.robux.data.input.impl.TimestampSpec;
import org.apache.robux.emitter.kafka.KafkaEmitter;
import org.apache.robux.emitter.kafka.KafkaEmitterModule;
import org.apache.robux.indexer.granularity.UniformGranularitySpec;
import org.apache.robux.indexing.compact.CompactionSupervisorSpec;
import org.apache.robux.indexing.kafka.KafkaIndexTaskModule;
import org.apache.robux.indexing.kafka.simulate.KafkaResource;
import org.apache.robux.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.robux.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.robux.indexing.kafka.supervisor.KafkaSupervisorTuningConfig;
import org.apache.robux.indexing.overlord.Segments;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.query.RobuxMetrics;
import org.apache.robux.rpc.UpdateResponse;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.segment.indexing.DataSchema;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordinator.ClusterCompactionConfig;
import org.apache.robux.server.coordinator.CoordinatorDynamicConfig;
import org.apache.robux.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.robux.testing.embedded.EmbeddedBroker;
import org.apache.robux.testing.embedded.EmbeddedClusterApis;
import org.apache.robux.testing.embedded.EmbeddedCoordinator;
import org.apache.robux.testing.embedded.EmbeddedRobuxCluster;
import org.apache.robux.testing.embedded.EmbeddedRobuxServer;
import org.apache.robux.testing.embedded.EmbeddedHistorical;
import org.apache.robux.testing.embedded.EmbeddedIndexer;
import org.apache.robux.testing.embedded.EmbeddedOverlord;
import org.apache.robux.testing.embedded.EmbeddedRouter;
import org.apache.robux.testing.embedded.emitter.LatchableEmitterModule;
import org.apache.robux.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Simulation test to emit cluster metrics using a {@link KafkaEmitter} and then
 * ingest them back into the cluster with a {@code KafkaSupervisor}.
 */
@SuppressWarnings("resource")
public class EmbeddedKafkaClusterMetricsTest extends EmbeddedClusterTestBase
{
  private static final String TOPIC = EmbeddedClusterApis.createTestDatasourceName();

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private KafkaResource kafkaServer;

  @Override
  public EmbeddedRobuxCluster createCluster()
  {
    final EmbeddedRobuxCluster cluster = EmbeddedRobuxCluster.withEmbeddedDerbyAndZookeeper();

    kafkaServer = new KafkaResource()
    {
      @Override
      public void start()
      {
        super.start();
        createTopicWithPartitions(TOPIC, 10);
        cluster.addCommonProperty("robux.emitter.kafka.bootstrap.servers", kafkaServer.getBootstrapServerUrl());
        cluster.addCommonProperty("robux.emitter.kafka.metric.topic", TOPIC);
        cluster.addCommonProperty("robux.emitter.kafka.alert.topic", TOPIC);
      }

      @Override
      public void stop()
      {
        deleteTopic(TOPIC);
        super.stop();
      }
    };

    indexer.addProperty("robux.segment.handoff.pollDuration", "PT0.1s")
           .addProperty("robux.worker.capacity", "10");
    overlord.addProperty("robux.indexer.task.default.context", "{\"useConcurrentLocks\": true}")
            .addProperty("robux.manager.segments.useIncrementalCache", "ifSynced")
            .addProperty("robux.manager.segments.pollDuration", "PT0.1s")
            .addProperty("robux.manager.segments.killUnused.enabled", "true")
            .addProperty("robux.manager.segments.killUnused.bufferPeriod", "PT0.1s")
            .addProperty("robux.manager.segments.killUnused.dutyPeriod", "PT1s");
    coordinator.addProperty("robux.manager.segments.useIncrementalCache", "ifSynced");
    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(KafkaEmitterModule.class)
           .addExtension(LatchableEmitterModule.class)
           .addCommonProperty("robux.emitter", "composing")
           .addCommonProperty("robux.emitter.composing.emitters", "[\"latching\",\"kafka\"]")
           .addCommonProperty("robux.monitoring.emissionPeriod", "PT0.1s")
           .addCommonProperty("robux.monitoring.monitors", "[\"org.apache.robux.java.util.metrics.JvmMonitor\"]")
           .addResource(kafkaServer)
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(broker)
           .addServer(historical)
           .addServer(new EmbeddedRouter());

    return cluster;
  }

  @Test
  @Timeout(20)
  public void test_ingest10kRows_ofSelfClusterMetrics_andVerifyValues()
  {
    final int maxRowsPerSegment = 1000;
    final int expectedSegmentsHandedOff = 10;

    final int taskCount = 5;
    final int taskDurationMillis = 1_000;
    final int taskCompletionTimeoutMillis = 10_000;

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(
        supervisorId,
        taskCount,
        taskDurationMillis,
        taskCompletionTimeoutMillis,
        maxRowsPerSegment
    );

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(kafkaSupervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", supervisorId), startSupervisorResult);

    // Wait for segments to be handed off
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count")
                      .hasDimension(RobuxMetrics.DATASOURCE, List.of(dataSource)),
        agg -> agg.hasSumAtLeast(expectedSegmentsHandedOff)
    );

    // Verify number of segments and total number of rows in the datasource
    final int numSegments = Integer.parseInt(
        cluster.runSql("SELECT COUNT(*) FROM sys.segments WHERE datasource = '%s'", dataSource)
    );
    Assertions.assertTrue(numSegments >= expectedSegmentsHandedOff);

    final int numRows = Integer.parseInt(
        cluster.runSql("SELECT COUNT(*) FROM %s", dataSource)
    );
    Assertions.assertTrue(numRows >= expectedSegmentsHandedOff * maxRowsPerSegment);

    verifyIngestedMetricCountMatchesEmittedCount("jvm/pool/committed", coordinator);
    verifyIngestedMetricCountMatchesEmittedCount("coordinator/time", coordinator);

    // Suspend the supervisor
    cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(kafkaSupervisorSpec.createSuspendedSpec())
    );
  }

  @Test
  @Timeout(120)
  public void test_ingestClusterMetrics_withConcurrentCompactionSupervisor_andSkipKillOfUnusedSegments()
  {
    final int maxRowsPerSegment = 500;
    final int compactedMaxRowsPerSegment = 5000;

    final int taskCount = 2;
    final int taskDurationMillis = 500;
    final int taskCompletionTimeoutMillis = 5_000;

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(
        supervisorId,
        taskCount,
        taskDurationMillis,
        taskCompletionTimeoutMillis,
        maxRowsPerSegment
    );
    cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(kafkaSupervisorSpec)
    );

    // Wait for some segments to be published
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("segment/txn/success")
                      .hasDimension(RobuxMetrics.DATASOURCE, dataSource)
    );

    // Enable compaction supervisors on the Overlord
    final ClusterCompactionConfig originalCompactionConfig = cluster.callApi().onLeaderOverlord(
        OverlordClient::getClusterCompactionConfig
    );

    final ClusterCompactionConfig updatedCompactionConfig
        = new ClusterCompactionConfig(1.0, 10, null, true, null);
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(updatedCompactionConfig)
    );
    Assertions.assertTrue(updateResponse.isSuccess());

    // Submit a compaction supervisor for this datasource
    final CompactionSupervisorSpec compactionSupervisorSpec = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withMaxRowsPerSegment(compactedMaxRowsPerSegment)
            .withTaskContext(Map.of("useConcurrentLocks", true))
            .build(),
        false,
        null
    );
    cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(compactionSupervisorSpec)
    );

    // Wait until some compaction tasks have finished
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(RobuxMetrics.TASK_TYPE, "compact")
                      .hasDimension(RobuxMetrics.TASK_STATUS, "SUCCESS"),
        agg -> agg.hasCountAtLeast(2)
    );

    // Verify that some segments have been upgraded due to Concurrent Append and Replace
    final Set<String> allUsedSegmentsIds = overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.INCLUDING_OVERSHADOWED)
        .stream()
        .map(s -> s.getId().toString())
        .collect(Collectors.toSet());
    final Map<String, String> upgradedFromSegmentIds = overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveUpgradedFromSegmentIds(dataSource, allUsedSegmentsIds);
    Assertions.assertFalse(upgradedFromSegmentIds.isEmpty());

    // Update Coordinator dynamic config to mark segments as unused as soon as they become overshadowed
    final CoordinatorDynamicConfig originalCoordinatorDynamicConfig = cluster.callApi().onLeaderCoordinator(
        CoordinatorClient::getCoordinatorDynamicConfig
    );
    final CoordinatorDynamicConfig updatedCoordinatorDynamicConfig
        = CoordinatorDynamicConfig.builder()
                                  .withMarkSegmentAsUnusedDelayMillis(10L)
                                  .build(originalCoordinatorDynamicConfig);
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(updatedCoordinatorDynamicConfig)
    );

    // Wait for some segments to become unused and be eligible for kill
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/kill/unusedIntervals/count")
                      .hasDimension(RobuxMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );

    // Verify that the segments are skipped since the interval is still being appended to
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/kill/skippedIntervals/count")
                      .hasDimension(RobuxMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );

    // Revert the cluster compaction config and coordinator dynamic config
    cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(originalCompactionConfig)
    );
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(originalCoordinatorDynamicConfig)
    );

    // Suspend the supervisors
    cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(compactionSupervisorSpec.createSuspendedSpec())
    );
    cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(kafkaSupervisorSpec.createSuspendedSpec())
    );
  }

  /**
   * SELECTs the total count of the given metric in the {@link #dataSource} and
   * verifies it against the metrics actually emitted by the server.
   */
  private void verifyIngestedMetricCountMatchesEmittedCount(String metricName, EmbeddedRobuxServer server)
  {
    // Get the value of the metric from the datasource
    final RobuxNode selfNode = server.bindings().selfNode();
    final int expectedValueForSegmentsAssigned = (int) Double.parseDouble(
        cluster.runSql(
            "SELECT COUNT(*) FROM %s WHERE metric = '%s' AND host = '%s' AND service = '%s'",
            dataSource, metricName, selfNode.getHostAndPort(), selfNode.getServiceName()
        )
    );
    Assertions.assertTrue(expectedValueForSegmentsAssigned > 0);

    // Verify the number of metrics actually emitted from this server
    server.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName(metricName),
        agg -> agg.hasCountAtLeast(expectedValueForSegmentsAssigned)
    );
  }

  private KafkaSupervisorSpec createKafkaSupervisor(
      String supervisorId,
      int taskCount,
      int taskDurationMillis,
      int taskCompletionTimeoutMillis,
      int maxRowsPerSegment
  )
  {
    final Period startDelay = Period.millis(10);
    final Period supervisorRunPeriod = Period.millis(500);
    final boolean useEarliestOffset = true;

    return new KafkaSupervisorSpec(
        supervisorId,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                  .withGranularity(new UniformGranularitySpec(Granularities.HOUR, null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .build(),
        createTuningConfig(maxRowsPerSegment),
        new KafkaSupervisorIOConfig(
            TOPIC,
            null,
            new JsonInputFormat(null, null, null, null, null),
            null,
            taskCount,
            Period.millis(taskDurationMillis),
            kafkaServer.consumerProperties(),
            null, null, null,
            startDelay,
            supervisorRunPeriod,
            useEarliestOffset,
            Period.millis(taskCompletionTimeoutMillis),
            null, null, null, null, null, null, null
        ),
        null, null, null, null, null, null, null, null, null, null, null
    );
  }

  private KafkaSupervisorTuningConfig createTuningConfig(int maxRowsPerSegment)
  {
    return new KafkaSupervisorTuningConfig(
        null,
        null, null, null,
        maxRowsPerSegment,
        null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null
    );
  }
}

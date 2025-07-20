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

package org.apache.robux.testing.embedded.msq;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.robux.data.input.impl.DimensionsSpec;
import org.apache.robux.data.input.impl.JsonInputFormat;
import org.apache.robux.data.input.impl.TimestampSpec;
import org.apache.robux.frame.testutil.FrameTestUtil;
import org.apache.robux.indexer.TaskStatusPlus;
import org.apache.robux.indexer.granularity.UniformGranularitySpec;
import org.apache.robux.indexing.kafka.KafkaIndexTaskModule;
import org.apache.robux.indexing.kafka.simulate.KafkaResource;
import org.apache.robux.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.robux.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.java.util.common.parsers.CloseableIterator;
import org.apache.robux.msq.dart.guice.DartControllerMemoryManagementModule;
import org.apache.robux.msq.dart.guice.DartControllerModule;
import org.apache.robux.msq.dart.guice.DartWorkerMemoryManagementModule;
import org.apache.robux.msq.dart.guice.DartWorkerModule;
import org.apache.robux.msq.guice.IndexerMemoryManagementModule;
import org.apache.robux.msq.guice.MSQDurableStorageModule;
import org.apache.robux.msq.guice.MSQIndexingModule;
import org.apache.robux.msq.guice.MSQSqlModule;
import org.apache.robux.msq.guice.SqlTaskModule;
import org.apache.robux.msq.indexing.report.MSQTaskReportPayload;
import org.apache.robux.query.RobuxMetrics;
import org.apache.robux.segment.QueryableIndexCursorFactory;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.segment.indexing.DataSchema;
import org.apache.robux.sql.calcite.BaseCalciteQueryTest;
import org.apache.robux.testing.embedded.EmbeddedBroker;
import org.apache.robux.testing.embedded.EmbeddedClusterApis;
import org.apache.robux.testing.embedded.EmbeddedCoordinator;
import org.apache.robux.testing.embedded.EmbeddedRobuxCluster;
import org.apache.robux.testing.embedded.EmbeddedHistorical;
import org.apache.robux.testing.embedded.EmbeddedIndexer;
import org.apache.robux.testing.embedded.EmbeddedOverlord;
import org.apache.robux.testing.embedded.EmbeddedRouter;
import org.apache.robux.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Embedded test to ingest {@link TestIndex#getMMappedWikipediaIndex()} into Kafka tasks, then query
 * those tasks with MSQ.
 */
public class EmbeddedMSQRealtimeQueryTest extends EmbeddedClusterTestBase
{
  private static final Period TASK_DURATION = Period.hours(1);
  private static final int TASK_COUNT = 2;

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final int totalRows = TestIndex.getMMappedWikipediaIndex().getNumRows();

  private KafkaResource kafka;
  private String topic;
  private EmbeddedMSQApis msqApis;

  @Override
  public EmbeddedRobuxCluster createCluster()
  {
    kafka = new KafkaResource();

    coordinator.addProperty("robux.manager.segments.useIncrementalCache", "always");

    overlord.addProperty("robux.manager.segments.useIncrementalCache", "always")
            .addProperty("robux.manager.segments.pollDuration", "PT0.1s");

    broker.addProperty("robux.msq.dart.controller.heapFraction", "0.9")
          .addProperty("robux.query.default.context.maxConcurrentStages", "1");

    historical.addProperty("robux.msq.dart.worker.heapFraction", "0.9")
              .addProperty("robux.msq.dart.worker.concurrentQueries", "1");

    indexer.setServerMemory(300_000_000) // to run 2x realtime and 2x MSQ tasks
           .addProperty("robux.segment.handoff.pollDuration", "PT0.1s")
           // robux.processing.numThreads must be higher than # of MSQ tasks to avoid contention, because the realtime
           // server is contacted in such a way that the processing thread is blocked
           .addProperty("robux.processing.numThreads", "3")
           .addProperty("robux.worker.capacity", "4");

    return EmbeddedRobuxCluster
        .withEmbeddedDerbyAndZookeeper()
        .addExtensions(
            KafkaIndexTaskModule.class,
            DartControllerModule.class,
            DartWorkerModule.class,
            DartControllerMemoryManagementModule.class,
            DartControllerModule.class,
            DartWorkerMemoryManagementModule.class,
            DartWorkerModule.class,
            IndexerMemoryManagementModule.class,
            MSQDurableStorageModule.class,
            MSQIndexingModule.class,
            MSQSqlModule.class,
            SqlTaskModule.class
        )
        .addCommonProperty("robux.monitoring.emissionPeriod", "PT0.1s")
        .addCommonProperty("robux.msq.dart.enabled", "true")
        .useLatchableEmitter()
        .addResource(kafka)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(broker)
        .addServer(historical)
        .addServer(router);
  }

  @BeforeEach
  void setUpEach()
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    topic = dataSource = EmbeddedClusterApis.createTestDatasourceName();

    // Create Kafka topic.
    kafka.createTopicWithPartitions(topic, 2);

    // Submit a supervisor.
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor();
    final Map<String, String> startSupervisorResult =
        cluster.callApi().onLeaderOverlord(o -> o.postSupervisor(kafkaSupervisorSpec));
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);

    // Send data to Kafka.
    final QueryableIndexCursorFactory wikiCursorFactory =
        new QueryableIndexCursorFactory(TestIndex.getMMappedWikipediaIndex());
    final RowSignature wikiSignature = wikiCursorFactory.getRowSignature();
    kafka.produceRecordsToTopic(
        FrameTestUtil.readRowsFromCursorFactory(wikiCursorFactory)
                     .map(row -> {
                       final Map<String, Object> rowMap = new LinkedHashMap<>();
                       for (int i = 0; i < row.size(); i++) {
                         rowMap.put(wikiSignature.getColumnName(i), row.get(i));
                       }
                       try {
                         return new ProducerRecord<>(
                             topic,
                             ByteArrays.EMPTY_ARRAY,
                             TestHelper.JSON_MAPPER.writeValueAsBytes(rowMap)
                         );
                       }
                       catch (JsonProcessingException e) {
                         throw new RuntimeException(e);
                       }
                     })
                     .toList()
    );

    // Wait for it to be loaded.
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(RobuxMetrics.DATASOURCE, Collections.singletonList(dataSource)),
        agg -> agg.hasSumAtLeast(totalRows)
    );
  }

  @AfterEach
  void tearDownEach() throws ExecutionException, InterruptedException, IOException
  {
    final Map<String, String> terminateSupervisorResult =
        cluster.callApi().onLeaderOverlord(o -> o.terminateSupervisor(dataSource));
    Assertions.assertEquals(Map.of("id", dataSource), terminateSupervisorResult);

    // Cancel all running tasks, so we don't need to wait for them to hand off their segments.
    try (final CloseableIterator<TaskStatusPlus> it = cluster.leaderOverlord().taskStatuses(null, null, null).get()) {
      while (it.hasNext()) {
        cluster.leaderOverlord().cancelTask(it.next().getId());
      }
    }

    kafka.deleteTopic(topic);
  }

  @Test
  @Timeout(60)
  public void test_selectCount_task_default()
  {
    final String sql = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource);
    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    // By default tasks do not include realtime data; count is zero.
    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        Collections.singletonList(new Object[]{0}),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_selectCount_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT COUNT(*) FROM \"%s\"",
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        Collections.singletonList(new Object[]{totalRows}),
        payload.getResults().getResults()
    );
  }

  @Test
  @Timeout(60)
  public void test_selectCount_dart_default()
  {
    final String sql = StringUtils.format("SELECT COUNT(*) FROM \"%s\"", dataSource);
    final long selectedCount = Long.parseLong(msqApis.runDartSql(sql));

    // By default Dart includes realtime data.
    Assertions.assertEquals(totalRows, selectedCount);
  }

  @Test
  @Timeout(60)
  public void test_selectCount_dart_noRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'NONE';\n"
        + "SELECT COUNT(*) FROM \"%s\"",
        dataSource
    );

    final long selectedCount = Long.parseLong(msqApis.runDartSql(sql));
    Assertions.assertEquals(0, selectedCount);
  }

  @Test
  @Timeout(60)
  @Disabled // Test does not currently pass, see https://github.com/apache/robux/issues/18198
  public void test_selectJoin_dart()
  {
    final long selectedCount = Long.parseLong(
        msqApis.runDartSql(
            "SELECT COUNT(*) FROM \"%s\"\n"
            + "WHERE countryName IN (\n"
            + "  SELECT countryName\n"
            + "  FROM \"%s\"\n"
            + "  WHERE countryName IS NOT NULL\n"
            + "  GROUP BY 1\n"
            + "  ORDER BY COUNT(*) DESC\n"
            + "  LIMIT 1\n"
            + ")",
            dataSource,
            dataSource
        )
    );

    Assertions.assertEquals(528, selectedCount);
  }

  @Test
  @Timeout(60)
  @Disabled // Test does not currently pass, see https://github.com/apache/robux/issues/18198
  public void test_selectJoin_task_withRealtime()
  {
    final String sql = StringUtils.format(
        "SET includeSegmentSource = 'REALTIME';\n"
        + "SELECT COUNT(*) FROM \"%s\"\n"
        + "WHERE countryName IN (\n"
        + "  SELECT countryName\n"
        + "  FROM \"%s\"\n"
        + "  WHERE countryName IS NOT NULL\n"
        + "  GROUP BY 1\n"
        + "  ORDER BY COUNT(*) DESC\n"
        + "  LIMIT 1\n"
        + ")",
        dataSource,
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        Collections.singletonList(new Object[]{528}),
        payload.getResults().getResults()
    );
  }

  private KafkaSupervisorSpec createKafkaSupervisor()
  {
    final Period startDelay = Period.millis(10);
    final Period supervisorRunPeriod = Period.millis(500);
    final boolean useEarliestOffset = true;

    return new KafkaSupervisorSpec(
        dataSource,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("__time", "auto", null))
                  .withGranularity(new UniformGranularitySpec(Granularities.DAY, null, null))
                  .withDimensions(DimensionsSpec.builder().useSchemaDiscovery(true).build())
                  .build(),
        null,
        new KafkaSupervisorIOConfig(
            topic,
            null,
            new JsonInputFormat(null, null, null, null, null),
            null,
            TASK_COUNT,
            TASK_DURATION,
            kafka.consumerProperties(),
            null,
            null,
            null,
            startDelay,
            supervisorRunPeriod,
            useEarliestOffset,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}

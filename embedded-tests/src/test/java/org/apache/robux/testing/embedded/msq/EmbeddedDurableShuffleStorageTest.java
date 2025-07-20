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

import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.robux.data.input.impl.LocalInputSource;
import org.apache.robux.indexer.TaskState;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.msq.dart.guice.DartControllerMemoryManagementModule;
import org.apache.robux.msq.dart.guice.DartControllerModule;
import org.apache.robux.msq.dart.guice.DartWorkerMemoryManagementModule;
import org.apache.robux.msq.dart.guice.DartWorkerModule;
import org.apache.robux.msq.exec.OutputChannelMode;
import org.apache.robux.msq.guice.IndexerMemoryManagementModule;
import org.apache.robux.msq.guice.MSQDurableStorageModule;
import org.apache.robux.msq.guice.MSQExternalDataSourceModule;
import org.apache.robux.msq.guice.MSQIndexingModule;
import org.apache.robux.msq.guice.MSQSqlModule;
import org.apache.robux.msq.guice.SqlTaskModule;
import org.apache.robux.msq.indexing.destination.MSQSelectDestination;
import org.apache.robux.msq.indexing.report.MSQStagesReport;
import org.apache.robux.msq.indexing.report.MSQTaskReportPayload;
import org.apache.robux.query.RobuxProcessingConfigTest;
import org.apache.robux.sql.calcite.BaseCalciteQueryTest;
import org.apache.robux.sql.calcite.planner.Calcites;
import org.apache.robux.storage.s3.output.S3StorageConnectorModule;
import org.apache.robux.testing.embedded.EmbeddedBroker;
import org.apache.robux.testing.embedded.EmbeddedClusterApis;
import org.apache.robux.testing.embedded.EmbeddedCoordinator;
import org.apache.robux.testing.embedded.EmbeddedRobuxCluster;
import org.apache.robux.testing.embedded.EmbeddedHistorical;
import org.apache.robux.testing.embedded.EmbeddedIndexer;
import org.apache.robux.testing.embedded.EmbeddedOverlord;
import org.apache.robux.testing.embedded.EmbeddedRouter;
import org.apache.robux.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.robux.testing.embedded.minio.MinIOStorageResource;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.shaded.com.google.common.io.ByteStreams;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Embedded test to batch-ingest wikipedia data from {@link #loadWikipediaTable()}, then query that data
 * with MSQ tasks using MinIO-based durable storage.
 */
public class EmbeddedDurableShuffleStorageTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private final EmbeddedRouter router = new EmbeddedRouter();
  private final MinIOStorageResource storageResource = new MinIOStorageResource();
  private final MinIODurableStorageResource msqStorageResource = new MinIODurableStorageResource(storageResource);

  private EmbeddedMSQApis msqApis;

  @Override
  public EmbeddedRobuxCluster createCluster()
  {
    coordinator.addProperty("robux.manager.segments.useIncrementalCache", "always");

    overlord.addProperty("robux.manager.segments.useIncrementalCache", "always")
            .addProperty("robux.manager.segments.pollDuration", "PT0.1s");

    indexer.setServerMemory(400_000_000)
           .addProperty("robux.worker.capacity", "4")
           .addProperty("robux.processing.numThreads", "3")
           .addProperty("robux.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedRobuxCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addExtensions(
            DartControllerModule.class,
            DartWorkerModule.class,
            DartControllerMemoryManagementModule.class,
            DartWorkerMemoryManagementModule.class,
            IndexerMemoryManagementModule.class,
            MSQDurableStorageModule.class,
            MSQIndexingModule.class,
            MSQSqlModule.class,
            SqlTaskModule.class,
            MSQExternalDataSourceModule.class,
            S3StorageConnectorModule.class
        )
        .addResource(storageResource)
        .addResource(msqStorageResource)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(broker)
        .addServer(historical)
        .addServer(router);
  }

  @BeforeAll
  final void setupData() throws IOException
  {
    msqApis = new EmbeddedMSQApis(cluster, overlord);
    dataSource = EmbeddedClusterApis.createTestDatasourceName();
    loadWikipediaTable();
  }

  @Override
  protected void beforeEachTest()
  {
    // do nothing here, the super version of this method generates a new value for dataSource field each time, but
    // we are setting that in our @BeforeAll where we are just inserting data once and re-using between runs
  }

  @Test
  @Timeout(120)
  public void test_selectFirstPage()
  {
    final String sql = StringUtils.format(
        "SET durableShuffleStorage = TRUE;\n"
        + "SET targetPartitionsPerWorker = 3;\n"
        + "SELECT __time, page \n"
        + "FROM \"%s\"\n"
        + "ORDER BY page ASC\n"
        + "LIMIT 1",
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        Collections.singletonList(new Object[]{DateTimes.of("2015-09-12T23:26:08.226Z").getMillis(), "!T.O.O.H.!"}),
        payload.getResults().getResults()
    );

    assertStageOutputIsDurable(payload, false);
  }

  @Test
  @Timeout(120)
  public void test_selectCount()
  {
    final String sql = StringUtils.format(
        "SET durableShuffleStorage = TRUE;\n"
        + "SET targetPartitionsPerWorker = 3;\n"
        + "SELECT COUNT(*) FROM \"%s\"",
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        Collections.singletonList(new Object[]{39244}),
        payload.getResults().getResults()
    );

    assertStageOutputIsDurable(payload, false);
  }

  @Test
  @Timeout(120)
  public void test_selectJoin_broadcast()
  {
    final String sql = StringUtils.format(
        "SET durableShuffleStorage = TRUE;\n"
        + "SET sqlJoinAlgorithm = 'broadcast';\n"
        + "SET targetPartitionsPerWorker = 3;\n"
        + "SELECT channel, COUNT(*)"
        + "FROM \"%s\""
        + "WHERE channel IN (SELECT channel FROM \"%s\" GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5)"
        + "GROUP BY channel\n"
        + "ORDER BY channel",
        dataSource,
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"#de.wikipedia", 2523},
            new Object[]{"#en.wikipedia", 11549},
            new Object[]{"#fr.wikipedia", 2099},
            new Object[]{"#ru.wikipedia", 1386},
            new Object[]{"#vi.wikipedia", 9747}
        ),
        payload.getResults().getResults()
    );

    assertStageOutputIsDurable(payload, false);
  }

  @Test
  @Timeout(120)
  public void test_selectJoin_sortMerge()
  {
    final String sql = StringUtils.format(
        "SET durableShuffleStorage = TRUE;\n"
        + "SET sqlJoinAlgorithm = 'sortMerge';\n"
        + "SET targetPartitionsPerWorker = 3;\n"
        + "SELECT channel, COUNT(*)"
        + "FROM \"%s\""
        + "WHERE channel IN (SELECT channel FROM \"%s\" GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5)"
        + "GROUP BY channel\n"
        + "ORDER BY channel",
        dataSource,
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"#de.wikipedia", 2523},
            new Object[]{"#en.wikipedia", 11549},
            new Object[]{"#fr.wikipedia", 2099},
            new Object[]{"#ru.wikipedia", 1386},
            new Object[]{"#vi.wikipedia", 9747}
        ),
        payload.getResults().getResults()
    );

    assertStageOutputIsDurable(payload, false);
  }

  @Test
  @Timeout(120)
  public void test_selectJoin_sortMerge_durableDestination()
  {
    final String sql = StringUtils.format(
        "SET durableShuffleStorage = TRUE;\n"
        + "SET sqlJoinAlgorithm = 'sortMerge';\n"
        + "SET targetPartitionsPerWorker = 3;\n"
        + "SET selectDestination = 'durableStorage';\n" // Write results to durable storage (instead of task report)
        + "SET maxNumTasks = 3;\n" // Force two worker tasks
        + "SET rowsPerPage = 3;\n" // Force two result partitions (one per task) -- results have 5 items
        + "SELECT channel, COUNT(*)"
        + "FROM \"%s\""
        + "WHERE channel IN (SELECT channel FROM \"%s\" GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5)"
        + "GROUP BY channel\n"
        + "ORDER BY channel",
        dataSource,
        dataSource
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);

    BaseCalciteQueryTest.assertResultsEquals(
        sql,
        List.of(
            new Object[]{"#de.wikipedia", 2523},
            new Object[]{"#en.wikipedia", 11549},
            new Object[]{"#fr.wikipedia", 2099},
            new Object[]{"#ru.wikipedia", 1386},
            new Object[]{"#vi.wikipedia", 9747}
        ),
        payload.getResults().getResults()
    );

    assertStageOutputIsDurable(payload, true);

    // Check that query results actually got written to durable storage.
    final List<MSQStagesReport.Stage> stages = payload.getStages().getStages();
    final String queryId = stages.get(0).getStageDefinition().getId().getQueryId();
    final String resultsBaseKey = StringUtils.format(
        "%s/query-results/controller_%s",
        msqStorageResource.getBaseKey(),
        queryId
    );

    final List<S3ObjectSummary> queryResultObjects =
        storageResource.getS3Client()
                       .listObjects(storageResource.getBucket(), resultsBaseKey)
                       .getObjectSummaries();

    final int lastStage = stages.size() - 1;
    Assertions.assertEquals(
        Set.of(
            StringUtils.format("%s/stage_%s/worker_0/__success", resultsBaseKey, lastStage),
            StringUtils.format("%s/stage_%s/worker_1/__success", resultsBaseKey, lastStage),
            StringUtils.format("%s/stage_%s/worker_0/taskId_%s-worker0_0/part_0", resultsBaseKey, lastStage, queryId),
            StringUtils.format("%s/stage_%s/worker_1/taskId_%s-worker1_0/part_1", resultsBaseKey, lastStage, queryId)
        ),
        queryResultObjects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toSet())
    );
  }

  /**
   * Insert wikipedia dataset into the table {@link #dataSource}.
   */
  private void loadWikipediaTable() throws IOException
  {
    final File tmpDir = cluster.getTestFolder().newFolder();
    final File wikiFile = new File(tmpDir, "wiki.gz");

    ByteStreams.copy(
        RobuxProcessingConfigTest.class.getResourceAsStream("/wikipedia/wikiticker-2015-09-12-sampled.json.gz"),
        Files.newOutputStream(wikiFile.toPath())
    );

    final String sql = StringUtils.format(
        "SET durableShuffleStorage = TRUE;"
        + "SET waitUntilSegmentsLoad = TRUE;\n"
        + "REPLACE INTO \"%s\" OVERWRITE ALL\n"
        + "SELECT\n"
        + "  TIME_PARSE(\"time\") AS __time,\n"
        + "  channel,\n"
        + "  countryName,\n"
        + "  page,\n"
        + "  \"user\",\n"
        + "  added,\n"
        + "  deleted,\n"
        + "  delta\n"
        + "FROM TABLE(\n"
        + "    EXTERN(\n"
        + "      %s,\n"
        + "      '{\"type\":\"json\"}',\n"
        + "      '[{\"name\":\"isRobot\",\"type\":\"string\"},{\"name\":\"channel\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"flags\",\"type\":\"string\"},{\"name\":\"isUnpatrolled\",\"type\":\"string\"},{\"name\":\"page\",\"type\":\"string\"},{\"name\":\"diffUrl\",\"type\":\"string\"},{\"name\":\"added\",\"type\":\"long\"},{\"name\":\"comment\",\"type\":\"string\"},{\"name\":\"commentLength\",\"type\":\"long\"},{\"name\":\"isNew\",\"type\":\"string\"},{\"name\":\"isMinor\",\"type\":\"string\"},{\"name\":\"delta\",\"type\":\"long\"},{\"name\":\"isAnonymous\",\"type\":\"string\"},{\"name\":\"user\",\"type\":\"string\"},{\"name\":\"deltaBucket\",\"type\":\"long\"},{\"name\":\"deleted\",\"type\":\"long\"},{\"name\":\"namespace\",\"type\":\"string\"},{\"name\":\"cityName\",\"type\":\"string\"},{\"name\":\"countryName\",\"type\":\"string\"},{\"name\":\"regionIsoCode\",\"type\":\"string\"},{\"name\":\"metroCode\",\"type\":\"long\"},{\"name\":\"countryIsoCode\",\"type\":\"string\"},{\"name\":\"regionName\",\"type\":\"string\"}]'\n"
        + "    )\n"
        + "  )\n"
        + "PARTITIONED BY DAY\n"
        + "CLUSTERED BY channel",
        dataSource,
        Calcites.escapeStringLiteral(
            broker.bindings()
                  .jsonMapper()
                  .writeValueAsString(new LocalInputSource(null, null, Collections.singletonList(wikiFile), null))
        )
    );

    final MSQTaskReportPayload payload = msqApis.runTaskSql(sql);
    Assertions.assertEquals(TaskState.SUCCESS, payload.getStatus().getStatus());
    Assertions.assertEquals(1, payload.getStatus().getSegmentLoadWaiterStatus().getTotalSegments());
    Assertions.assertNull(payload.getStatus().getErrorReport());
  }

  /**
   * Asserts that all stages report output channels of type {@link OutputChannelMode#DURABLE_STORAGE_INTERMEDIATE},
   * or possibly {@link OutputChannelMode#DURABLE_STORAGE_QUERY_RESULTS} for the final stage if {@code durableResults}.
   * the destination is {@link MSQSelectDestination#DURABLESTORAGE}.
   */
  private static void assertStageOutputIsDurable(
      final MSQTaskReportPayload reportPayload,
      final boolean durableResults
  )
  {
    final List<MSQStagesReport.Stage> stages = reportPayload.getStages().getStages();
    MatcherAssert.assertThat(stages.size(), Matchers.greaterThanOrEqualTo(1));

    for (final MSQStagesReport.Stage stage : stages) {
      final OutputChannelMode expectedOutputChannelMode;
      if (stage.getStageNumber() == stages.size() - 1 && durableResults) {
        expectedOutputChannelMode = OutputChannelMode.DURABLE_STORAGE_QUERY_RESULTS;
      } else {
        expectedOutputChannelMode = OutputChannelMode.DURABLE_STORAGE_INTERMEDIATE;
      }

      Assertions.assertEquals(
          expectedOutputChannelMode,
          stage.getOutputChannelMode(),
          StringUtils.format("OutputChannelMode for stage[%s]", stage.getStageNumber())
      );
    }
  }
}

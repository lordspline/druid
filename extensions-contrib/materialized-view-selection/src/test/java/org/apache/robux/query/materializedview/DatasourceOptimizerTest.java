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

package org.apache.robux.query.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.robux.client.BatchServerInventoryView;
import org.apache.robux.client.BrokerSegmentWatcherConfig;
import org.apache.robux.client.BrokerServerView;
import org.apache.robux.client.BrokerViewOfCoordinatorConfig;
import org.apache.robux.client.DirectRobuxClientFactory;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.robux.client.selector.RandomServerSelectorStrategy;
import org.apache.robux.curator.CuratorTestBase;
import org.apache.robux.indexing.materializedview.DerivativeDataSourceMetadata;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.robux.metadata.TestDerbyConnector;
import org.apache.robux.metadata.segment.SqlSegmentMetadataTransactionFactory;
import org.apache.robux.metadata.segment.cache.NoopSegmentMetadataCache;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QueryRunnerTestHelper;
import org.apache.robux.query.QueryWatcher;
import org.apache.robux.query.aggregation.LongSumAggregatorFactory;
import org.apache.robux.query.spec.MultipleIntervalSegmentSpec;
import org.apache.robux.query.topn.TopNQuery;
import org.apache.robux.query.topn.TopNQueryBuilder;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.segment.metadata.SegmentSchemaManager;
import org.apache.robux.segment.realtime.appenderator.SegmentSchemas;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordination.TestCoordinatorClient;
import org.apache.robux.server.coordinator.simulate.TestRobuxLeaderSelector;
import org.apache.robux.server.initialization.ZkPathsConfig;
import org.apache.robux.server.metrics.NoopServiceEmitter;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class DatasourceOptimizerTest extends CuratorTestBase
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private DerivativeDataSourceManager derivativesManager;
  private RobuxServer robuxServer;
  private ObjectMapper jsonMapper;
  private ZkPathsConfig zkPathsConfig;
  private DataSourceOptimizer optimizer;
  private IndexerSQLMetadataStorageCoordinator metadataStorageCoordinator;
  private BatchServerInventoryView baseView;
  private BrokerServerView brokerServerView;
  private SegmentSchemaManager segmentSchemaManager;

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createDataSourceTable();
    derbyConnector.createSegmentSchemasTable();
    derbyConnector.createSegmentTable();
    MaterializedViewConfig viewConfig = new MaterializedViewConfig();
    jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerSubtypes(new NamedType(DerivativeDataSourceMetadata.class, "view"));
    metadataStorageCoordinator = EasyMock.createMock(IndexerSQLMetadataStorageCoordinator.class);
    derivativesManager = new DerivativeDataSourceManager(
        viewConfig,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        jsonMapper,
        derbyConnector
    );
    segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        jsonMapper,
        derbyConnector
    );

    metadataStorageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        new SqlSegmentMetadataTransactionFactory(
            jsonMapper,
            derbyConnectorRule.metadataTablesConfigSupplier().get(),
            derbyConnector,
            new TestRobuxLeaderSelector(),
            NoopSegmentMetadataCache.instance(),
            NoopServiceEmitter.instance()
        ),
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnector,
        segmentSchemaManager,
        CentralizedDatasourceSchemaConfig.create()
    );

    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();

    zkPathsConfig = new ZkPathsConfig();
    setupViews();

    robuxServer = new RobuxServer(
        "localhost:1234",
        "localhost:1234",
        null,
        10000000L,
        ServerType.HISTORICAL,
        "default_tier",
        0
    );
    setupZNodeForServer(robuxServer, new ZkPathsConfig(), jsonMapper);
    optimizer = new DataSourceOptimizer(brokerServerView);
  }

  @After
  public void tearDown() throws IOException
  {
    baseView.stop();
    tearDownServerAndCurator();
  }

  @Test(timeout = 60_000L)
  public void testOptimize() throws InterruptedException
  {
    // insert datasource metadata
    String dataSource = "derivative";
    String baseDataSource = "base";
    Set<String> dims = Sets.newHashSet("dim1", "dim2", "dim3");
    Set<String> metrics = Sets.newHashSet("cost");
    DerivativeDataSourceMetadata metadata = new DerivativeDataSourceMetadata(baseDataSource, dims, metrics);
    metadataStorageCoordinator.insertDataSourceMetadata(dataSource, metadata);
    // insert base datasource segments
    List<Boolean> baseResult = Lists.transform(
        ImmutableList.of(
            "2011-04-01/2011-04-02",
            "2011-04-02/2011-04-03",
            "2011-04-03/2011-04-04",
            "2011-04-04/2011-04-05",
            "2011-04-05/2011-04-06"
        ),
        interval -> {
          final DataSegment segment = createDataSegment(
              "base",
              interval,
              "v1",
              Lists.newArrayList("dim1", "dim2", "dim3", "dim4"),
              1024 * 1024
          );
          metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
          announceSegmentForServer(robuxServer, segment, zkPathsConfig, jsonMapper);
          return true;
        }
    );
    // insert derivative segments
    List<Boolean> derivativeResult = Lists.transform(
        ImmutableList.of(
            "2011-04-01/2011-04-02",
            "2011-04-02/2011-04-03",
            "2011-04-03/2011-04-04"
        ),
        interval -> {
          final DataSegment segment = createDataSegment(
              "derivative",
              interval,
              "v1",
              Lists.newArrayList("dim1", "dim2", "dim3"),
              1024
          );
          metadataStorageCoordinator.commitSegments(Sets.newHashSet(segment), null);
          announceSegmentForServer(robuxServer, segment, zkPathsConfig, jsonMapper);
          return true;
        }
    );
    Assert.assertFalse(baseResult.contains(false));
    Assert.assertFalse(derivativeResult.contains(false));
    derivativesManager.start();
    while (DerivativeDataSourceManager.getAllDerivatives().isEmpty()) {
      TimeUnit.SECONDS.sleep(1L);
    }
    // build user query
    TopNQuery userQuery = new TopNQueryBuilder()
        .dataSource("base")
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("dim1")
        .metric("cost")
        .threshold(4)
        .intervals("2011-04-01/2011-04-06")
        .aggregators(new LongSumAggregatorFactory("cost", "cost"))
        .build();

    List<Query> expectedQueryAfterOptimizing = Lists.newArrayList(
        new TopNQueryBuilder()
            .dataSource("derivative")
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-01/2011-04-04"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build(),
        new TopNQueryBuilder()
            .dataSource("base")
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension("dim1")
            .metric("cost")
            .threshold(4)
            .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-04/2011-04-06"))))
            .aggregators(new LongSumAggregatorFactory("cost", "cost"))
            .build()
    );
    Assert.assertEquals(expectedQueryAfterOptimizing, optimizer.optimize(userQuery));
    derivativesManager.stop();
  }

  private DataSegment createDataSegment(String name, String intervalStr, String version, List<String> dims, long size)
  {
    return DataSegment.builder()
                      .dataSource(name)
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
                      .dimensions(dims)
                      .metrics(ImmutableList.of("cost"))
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(size)
                      .build();
  }

  private void setupViews() throws Exception
  {
    baseView = new BatchServerInventoryView(zkPathsConfig, curator, jsonMapper, Predicates.alwaysTrue(), "test")
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
                return callback.segmentAdded(server, segment);
              }

              @Override
              public CallbackAction segmentRemoved(RobuxServerMetadata server, DataSegment segment)
              {
                return callback.segmentRemoved(server, segment);
              }

              @Override
              public CallbackAction segmentViewInitialized()
              {
                return callback.segmentViewInitialized();
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

    BrokerViewOfCoordinatorConfig filter = new BrokerViewOfCoordinatorConfig(new TestCoordinatorClient());
    filter.start();
    brokerServerView = new BrokerServerView(
        robuxClientFactory,
        baseView,
        new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()),
        new NoopServiceEmitter(),
        new BrokerSegmentWatcherConfig(),
        filter
    );
    baseView.start();
  }

  private ObjectMapper getSmileMapper()
  {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    final ObjectMapper retVal = new DefaultObjectMapper(smileFactory, "broker");
    retVal.getFactory().setCodec(retVal);
    return retVal;
  }
}

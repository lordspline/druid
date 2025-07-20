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
import org.apache.robux.common.config.ConfigManager;
import org.apache.robux.common.config.JacksonConfigManager;
import org.apache.robux.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.metadata.MetadataStorageConnector;
import org.apache.robux.metadata.MetadataStorageTablesConfig;
import org.apache.robux.server.coordinator.CoordinatorConfigManager;
import org.apache.robux.server.coordinator.DataSourceCompactionConfig;
import org.apache.robux.server.coordinator.RobuxCompactionConfig;
import org.apache.robux.server.coordinator.RobuxCoordinatorRuntimeParams;
import org.apache.robux.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.robux.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.robux.server.coordinator.config.MetadataCleanupConfig;
import org.apache.robux.server.coordinator.stats.CoordinatorRunStats;
import org.apache.robux.server.coordinator.stats.Stats;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KillCompactionConfigTest
{
  @Mock
  private RobuxCoordinatorRuntimeParams mockRobuxCoordinatorRuntimeParams;

  @Mock
  private IndexerMetadataStorageCoordinator mockStorageCoordinator;

  @Mock
  private JacksonConfigManager mockJacksonConfigManager;

  @Mock
  private MetadataStorageConnector mockConnector;

  @Mock
  private MetadataStorageTablesConfig mockConnectorConfig;

  private CoordinatorConfigManager coordinatorConfigManager;
  private KillCompactionConfig killCompactionConfig;
  private CoordinatorRunStats runStats;

  @Before
  public void setup()
  {
    runStats = new CoordinatorRunStats();
    Mockito.when(mockConnectorConfig.getConfigTable()).thenReturn("robux_config");
    Mockito.when(mockRobuxCoordinatorRuntimeParams.getCoordinatorStats()).thenReturn(runStats);
    coordinatorConfigManager = new CoordinatorConfigManager(
        mockJacksonConfigManager,
        mockConnector,
        mockConnectorConfig,
        null
    );
  }

  @Test
  public void testRunSkipIfLastRunLessThanPeriod()
  {
    final MetadataCleanupConfig config
        = new MetadataCleanupConfig(true, new Duration(Long.MAX_VALUE), null);
    killCompactionConfig = new KillCompactionConfig(
        config,
        mockStorageCoordinator,
        coordinatorConfigManager
    );
    killCompactionConfig.run(mockRobuxCoordinatorRuntimeParams);
    Mockito.verifyNoInteractions(mockStorageCoordinator);
    Mockito.verifyNoInteractions(mockJacksonConfigManager);
    Assert.assertEquals(0, runStats.rowCount());
  }

  @Test
  public void testRunDoNothingIfCurrentConfigIsEmpty()
  {
    // Set current compaction config to an empty compaction config
    Mockito.when(mockConnector.lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY))
    ).thenReturn(null);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
        ArgumentMatchers.eq(null),
        ArgumentMatchers.eq(RobuxCompactionConfig.class),
        ArgumentMatchers.eq(RobuxCompactionConfig.empty()))
    ).thenReturn(RobuxCompactionConfig.empty());

    final MetadataCleanupConfig config
        = new MetadataCleanupConfig(true, new Duration("PT6S"), null);
    killCompactionConfig = new KillCompactionConfig(
        config,
        mockStorageCoordinator,
        coordinatorConfigManager
    );
    killCompactionConfig.run(mockRobuxCoordinatorRuntimeParams);
    Mockito.verifyNoInteractions(mockStorageCoordinator);
    Assert.assertTrue(runStats.hasStat(Stats.Kill.COMPACTION_CONFIGS));
    Assert.assertEquals(0, runStats.get(Stats.Kill.COMPACTION_CONFIGS));

    Mockito.verify(mockJacksonConfigManager).convertByteToConfig(
        ArgumentMatchers.eq(null),
        ArgumentMatchers.eq(RobuxCompactionConfig.class),
        ArgumentMatchers.eq(RobuxCompactionConfig.empty())
    );
    Mockito.verify(mockConnector).lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY)
    );
    Mockito.verifyNoMoreInteractions(mockJacksonConfigManager);
  }

  @Test
  public void testRunRemoveInactiveDatasourceCompactionConfig()
  {
    String inactiveDatasourceName = "inactive_datasource";
    String activeDatasourceName = "active_datasource";
    DataSourceCompactionConfig inactiveDatasourceConfig =
        InlineSchemaDataSourceCompactionConfig.builder()
                                              .forDataSource(inactiveDatasourceName)
                                              .withInputSegmentSizeBytes(500L)
                                              .withSkipOffsetFromLatest(new Period(3600))
                                              .withGranularitySpec(
                                            new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
                                        )
                                              .withTaskContext(ImmutableMap.of("key", "val"))
                                              .build();

    DataSourceCompactionConfig activeDatasourceConfig
        = InlineSchemaDataSourceCompactionConfig.builder()
                                                .forDataSource(activeDatasourceName)
                                                .withInputSegmentSizeBytes(500L)
                                                .withSkipOffsetFromLatest(new Period(3600))
                                                .withGranularitySpec(
                                              new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
                                          )
                                                .withTaskContext(ImmutableMap.of("key", "val"))
                                                .build();
    RobuxCompactionConfig originalCurrentConfig = RobuxCompactionConfig.empty().withDatasourceConfigs(
            ImmutableList.of(inactiveDatasourceConfig, activeDatasourceConfig)
    );
    byte[] originalCurrentConfigBytes = {1, 2, 3};
    Mockito.when(mockConnector.lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY))
    ).thenReturn(originalCurrentConfigBytes);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
        ArgumentMatchers.eq(originalCurrentConfigBytes),
        ArgumentMatchers.eq(RobuxCompactionConfig.class),
        ArgumentMatchers.eq(RobuxCompactionConfig.empty()))
    ).thenReturn(originalCurrentConfig);
    Mockito.when(mockStorageCoordinator.retrieveAllDatasourceNames()).thenReturn(ImmutableSet.of(activeDatasourceName));
    final ArgumentCaptor<byte[]> oldConfigCaptor = ArgumentCaptor.forClass(byte[].class);
    final ArgumentCaptor<RobuxCompactionConfig> newConfigCaptor = ArgumentCaptor.forClass(RobuxCompactionConfig.class);
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY),
        oldConfigCaptor.capture(),
        newConfigCaptor.capture(),
        ArgumentMatchers.any())
    ).thenReturn(ConfigManager.SetResult.ok());

    final MetadataCleanupConfig config
        = new MetadataCleanupConfig(true, new Duration("PT6S"), null);
    killCompactionConfig = new KillCompactionConfig(
        config,
        mockStorageCoordinator,
        coordinatorConfigManager
    );
    killCompactionConfig.run(mockRobuxCoordinatorRuntimeParams);

    // Verify and Assert
    Assert.assertNotNull(oldConfigCaptor.getValue());
    Assert.assertEquals(oldConfigCaptor.getValue(), originalCurrentConfigBytes);
    Assert.assertNotNull(newConfigCaptor.getValue());
    // The updated config should only contains one compaction config for the active datasource
    Assert.assertEquals(1, newConfigCaptor.getValue().getCompactionConfigs().size());

    Assert.assertEquals(activeDatasourceConfig, newConfigCaptor.getValue().getCompactionConfigs().get(0));
    Assert.assertEquals(1, runStats.get(Stats.Kill.COMPACTION_CONFIGS));

    Mockito.verify(mockJacksonConfigManager).convertByteToConfig(
        ArgumentMatchers.eq(originalCurrentConfigBytes),
        ArgumentMatchers.eq(RobuxCompactionConfig.class),
        ArgumentMatchers.eq(RobuxCompactionConfig.empty())
    );
    Mockito.verify(mockConnector).lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY)
    );
    Mockito.verify(mockJacksonConfigManager).set(
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.any(byte[].class),
        ArgumentMatchers.any(RobuxCompactionConfig.class),
        ArgumentMatchers.any()
    );
    Mockito.verifyNoMoreInteractions(mockJacksonConfigManager);
    Mockito.verify(mockStorageCoordinator).retrieveAllDatasourceNames();
    Mockito.verifyNoMoreInteractions(mockStorageCoordinator);
  }

  @Test
  public void testRunRetryForRetryableException()
  {
    String inactiveDatasourceName = "inactive_datasource";
    DataSourceCompactionConfig inactiveDatasourceConfig =
        InlineSchemaDataSourceCompactionConfig.builder()
                                              .forDataSource(inactiveDatasourceName)
                                              .withInputSegmentSizeBytes(500L)
                                              .withSkipOffsetFromLatest(new Period(3600))
                                              .withGranularitySpec(
                                            new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null)
                                        )
                                              .withTaskContext(ImmutableMap.of("key", "val"))
                                              .build();

    RobuxCompactionConfig originalCurrentConfig = RobuxCompactionConfig.empty().withDatasourceConfigs(
        ImmutableList.of(inactiveDatasourceConfig)
    );
    byte[] originalCurrentConfigBytes = {1, 2, 3};
    Mockito.when(mockConnector.lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY))
    ).thenReturn(originalCurrentConfigBytes);
    Mockito.when(mockJacksonConfigManager.convertByteToConfig(
        ArgumentMatchers.eq(originalCurrentConfigBytes),
        ArgumentMatchers.eq(RobuxCompactionConfig.class),
        ArgumentMatchers.eq(RobuxCompactionConfig.empty()))
    ).thenReturn(originalCurrentConfig);
    Mockito.when(mockStorageCoordinator.retrieveAllDatasourceNames()).thenReturn(ImmutableSet.of());
    Mockito.when(mockJacksonConfigManager.set(
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.any(byte[].class),
        ArgumentMatchers.any(RobuxCompactionConfig.class),
        ArgumentMatchers.any())
    ).thenReturn(
        // Return fail result with RetryableException the first three calls to updated set
        ConfigManager.SetResult.retryableFailure(new Exception()),
        ConfigManager.SetResult.retryableFailure(new Exception()),
        ConfigManager.SetResult.retryableFailure(new Exception()),
        // Return success ok on the fourth call to set updated config
        ConfigManager.SetResult.ok()
    );

    final MetadataCleanupConfig config
        = new MetadataCleanupConfig(true, new Duration("PT6S"), null);
    killCompactionConfig = new KillCompactionConfig(
        config,
        mockStorageCoordinator,
        coordinatorConfigManager
    );
    killCompactionConfig.run(mockRobuxCoordinatorRuntimeParams);

    // Verify that 1 config has been deleted
    Assert.assertEquals(1, runStats.get(Stats.Kill.COMPACTION_CONFIGS));

    // Should call convertByteToConfig and lookup (to refresh current compaction config) four times due to RetryableException when failed
    Mockito.verify(mockJacksonConfigManager, Mockito.times(4)).convertByteToConfig(
        ArgumentMatchers.eq(originalCurrentConfigBytes),
        ArgumentMatchers.eq(RobuxCompactionConfig.class),
        ArgumentMatchers.eq(RobuxCompactionConfig.empty())
    );
    Mockito.verify(mockConnector, Mockito.times(4)).lookup(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.eq("name"),
        ArgumentMatchers.eq("payload"),
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY)
    );

    // Should call set (to try set new updated compaction config) four times due to RetryableException when failed
    Mockito.verify(mockJacksonConfigManager, Mockito.times(4)).set(
        ArgumentMatchers.eq(RobuxCompactionConfig.CONFIG_KEY),
        ArgumentMatchers.any(byte[].class),
        ArgumentMatchers.any(RobuxCompactionConfig.class),
        ArgumentMatchers.any()
    );
    Mockito.verifyNoMoreInteractions(mockJacksonConfigManager);
    // Should call retrieveAllDataSourceNames four times due to RetryableException when failed
    Mockito.verify(mockStorageCoordinator, Mockito.times(4)).retrieveAllDatasourceNames();
    Mockito.verifyNoMoreInteractions(mockStorageCoordinator);
  }
}

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

package org.apache.robux.tests.coordinator.duty;

import com.google.inject.Inject;
import org.apache.robux.data.input.MaxSizeSplitHintSpec;
import org.apache.robux.indexer.partitions.DynamicPartitionsSpec;
import org.apache.robux.indexer.partitions.PartitionsSpec;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.server.coordinator.DataSourceCompactionConfig;
import org.apache.robux.server.coordinator.RobuxCompactionConfig;
import org.apache.robux.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.robux.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.robux.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.robux.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.robux.testing.IntegrationTestingConfig;
import org.apache.robux.testing.clients.CompactionResourceTestClient;
import org.apache.robux.testing.guice.RobuxTestModuleFactory;
import org.apache.robux.tests.TestNGGroup;
import org.apache.robux.tests.indexer.AbstractIndexerTest;
import org.joda.time.Period;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = {TestNGGroup.UPGRADE})
@Guice(moduleFactory = RobuxTestModuleFactory.class)
public class ITAutoCompactionUpgradeTest extends AbstractIndexerTest
{
  private static final String UPGRADE_DATASOURCE_NAME = "upgradeTest";

  @Inject
  protected CompactionResourceTestClient compactionResource;

  @Inject
  private IntegrationTestingConfig config;

  @Test
  public void testUpgradeAutoCompactionConfigurationWhenConfigurationFromOlderVersionAlreadyExist() throws Exception
  {
    // Verify that compaction config already exist. This config was inserted manually into the database using SQL script.
    // This auto compaction configuration payload is from Robux 0.21.0
    RobuxCompactionConfig coordinatorCompactionConfig = RobuxCompactionConfig.empty()
        .withDatasourceConfigs(compactionResource.getAllCompactionConfigs());
    DataSourceCompactionConfig foundDataSourceCompactionConfig
        = coordinatorCompactionConfig.findConfigForDatasource(UPGRADE_DATASOURCE_NAME).orNull();
    Assert.assertNotNull(foundDataSourceCompactionConfig);

    // Now submit a new auto compaction configuration
    PartitionsSpec newPartitionsSpec = new DynamicPartitionsSpec(4000, null);
    Period newSkipOffset = Period.seconds(0);

    DataSourceCompactionConfig compactionConfig = InlineSchemaDataSourceCompactionConfig
        .builder()
        .forDataSource(UPGRADE_DATASOURCE_NAME)
        .withSkipOffsetFromLatest(newSkipOffset)
        .withTuningConfig(
            new UserCompactionTaskQueryTuningConfig(
                null,
                null,
                null,
                null,
                new MaxSizeSplitHintSpec(null, 1),
                newPartitionsSpec,
                null,
                null,
                null,
                null,
                null,
                1,
                null,
                null,
                null,
                null,
                null,
                1,
                null
            )
        )
        .withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null)
        )
        .withIoConfig(new UserCompactionTaskIOConfig(true))
        .build();
    compactionResource.submitCompactionConfig(compactionConfig);

    // Verify that compaction was successfully updated
    foundDataSourceCompactionConfig
        = compactionResource.getDataSourceCompactionConfig(UPGRADE_DATASOURCE_NAME);
    Assert.assertNotNull(foundDataSourceCompactionConfig);
    Assert.assertNotNull(foundDataSourceCompactionConfig.getTuningConfig());
    Assert.assertEquals(foundDataSourceCompactionConfig.getTuningConfig().getPartitionsSpec(), newPartitionsSpec);
    Assert.assertEquals(foundDataSourceCompactionConfig.getSkipOffsetFromLatest(), newSkipOffset);
  }
}

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

package org.apache.robux.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.metadata.MetadataStorageTablesConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

public class MetadataStorageTablesConfigTest
{
  @Test
  public void testSerdeMetadataStorageTablesConfig()
  {
    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.install(new PropertiesModule(Collections.singletonList("test.runtime.properties")));
            binder.install(new ConfigModule());
            binder.install(new RobuxGuiceExtensions());
            JsonConfigProvider.bind(binder, "robux.metadata.storage.tables", MetadataStorageTablesConfig.class);
          }

          @Provides
          @LazySingleton
          public ObjectMapper jsonMapper()
          {
            return new DefaultObjectMapper();
          }
        }
    );

    Properties props = injector.getInstance(Properties.class);
    MetadataStorageTablesConfig config = injector.getInstance(MetadataStorageTablesConfig.class);

    Assert.assertEquals(props.getProperty("robux.metadata.storage.tables.base"), config.getBase());
    Assert.assertEquals(props.getProperty("robux.metadata.storage.tables.segments"), config.getSegmentsTable());
    Assert.assertEquals(props.getProperty("robux.metadata.storage.tables.segmentSchemas"), config.getSegmentSchemasTable());
    Assert.assertEquals(props.getProperty("robux.metadata.storage.tables.rules"), config.getRulesTable());
    Assert.assertEquals(props.getProperty("robux.metadata.storage.tables.config"), config.getConfigTable());
    Assert.assertEquals(
        props.getProperty("robux.metadata.storage.tables.tasks"),
        config.getEntryTable(MetadataStorageTablesConfig.TASK_ENTRY_TYPE)
    );
    Assert.assertEquals(
        props.getProperty("robux.metadata.storage.tables.taskLog"),
        config.getLogTable(MetadataStorageTablesConfig.TASK_ENTRY_TYPE)
    );
    Assert.assertEquals(
        props.getProperty("robux.metadata.storage.tables.taskLock"),
        config.getLockTable(MetadataStorageTablesConfig.TASK_ENTRY_TYPE)
    );
    Assert.assertEquals(props.getProperty("robux.metadata.storage.tables.dataSource"), config.getDataSourceTable());
    Assert.assertEquals(props.getProperty("robux.metadata.storage.tables.supervisors"), config.getSupervisorTable());
    Assert.assertEquals(props.getProperty("robux.metadata.storage.tables.upgradeSegments"), config.getUpgradeSegmentsTable());
  }

  @Test
  public void testReadConfig()
  {
    MetadataStorageTablesConfig fromBase = MetadataStorageTablesConfig.fromBase("robux.metadata.storage.tables");
    Assert.assertEquals("robux.metadata.storage.tables_segments", fromBase.getSegmentsTable());
    Assert.assertEquals("robux.metadata.storage.tables_segmentSchemas", fromBase.getSegmentSchemasTable());
    Assert.assertEquals("robux.metadata.storage.tables_tasklocks", fromBase.getTaskLockTable());
    Assert.assertEquals("robux.metadata.storage.tables_rules", fromBase.getRulesTable());
    Assert.assertEquals("robux.metadata.storage.tables_config", fromBase.getConfigTable());
    Assert.assertEquals("robux.metadata.storage.tables_dataSource", fromBase.getDataSourceTable());
    Assert.assertEquals("robux.metadata.storage.tables_supervisors", fromBase.getSupervisorTable());
    Assert.assertEquals("robux.metadata.storage.tables_upgradeSegments", fromBase.getUpgradeSegmentsTable());
  }
}

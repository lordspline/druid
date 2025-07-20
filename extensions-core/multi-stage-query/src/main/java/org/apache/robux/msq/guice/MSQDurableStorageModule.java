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

package org.apache.robux.msq.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.multibindings.Multibinder;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.indexing.overlord.duty.OverlordDuty;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.msq.indexing.cleaner.DurableStorageCleaner;
import org.apache.robux.msq.indexing.cleaner.DurableStorageCleanerConfig;
import org.apache.robux.storage.NilStorageConnector;
import org.apache.robux.storage.StorageConnectorProvider;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Module for functionality related to durable storage for stage output data.
 */
public class MSQDurableStorageModule implements RobuxModule
{
  public static final String MSQ_INTERMEDIATE_STORAGE_PREFIX =
      String.join(".", MSQIndexingModule.BASE_MSQ_KEY, "intermediate.storage");

  public static final String MSQ_INTERMEDIATE_STORAGE_ENABLED =
      String.join(".", MSQ_INTERMEDIATE_STORAGE_PREFIX, "enable");

  private Properties properties;
  private Set<NodeRole> nodeRoles;

  @Inject
  public void setProperties(Properties properties)
  {
    this.properties = properties;
  }

  @Inject
  public void setNodeRoles(@Self Set<NodeRole> nodeRoles)
  {
    this.nodeRoles = nodeRoles;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    if (isDurableShuffleStorageEnabled()) {
      JsonConfigProvider.bind(
          binder,
          MSQ_INTERMEDIATE_STORAGE_PREFIX,
          StorageConnectorProvider.class,
          MultiStageQuery.class
      );

      if (nodeRoles.contains(NodeRole.OVERLORD)) {
        JsonConfigProvider.bind(
            binder,
            String.join(".", MSQ_INTERMEDIATE_STORAGE_PREFIX, "cleaner"),
            DurableStorageCleanerConfig.class
        );

        Multibinder.newSetBinder(binder, OverlordDuty.class)
                   .addBinding()
                   .to(DurableStorageCleaner.class);
      }
    } else {
      // bind with nil implementation so that configs are not required during service startups.
      binder.bind(Key.get(StorageConnectorProvider.class, MultiStageQuery.class)).toInstance(tempDir -> NilStorageConnector.getInstance());
    }
  }

  private boolean isDurableShuffleStorageEnabled()
  {
    return Boolean.parseBoolean(properties.getProperty(MSQ_INTERMEDIATE_STORAGE_ENABLED, "false"));
  }
}

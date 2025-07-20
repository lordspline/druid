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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.robux.indexing.common.task.Task;
import org.apache.robux.indexing.overlord.GlobalTaskLockbox;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.testing.cluster.ClusterTestingTaskConfig;
import org.apache.robux.testing.cluster.overlord.FaultyLagAggregator;
import org.apache.robux.testing.cluster.overlord.FaultyTaskLockbox;
import org.apache.robux.testing.cluster.task.FaultyCoordinatorClient;
import org.apache.robux.testing.cluster.task.FaultyOverlordClient;
import org.apache.robux.testing.cluster.task.FaultyRemoteTaskActionClientFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Module that injects faulty clients into the Peon process to simulate various
 * fault scenarios.
 */
public class ClusterTestingModule implements RobuxModule
{
  private static final Logger log = new Logger(ClusterTestingModule.class);

  private Set<NodeRole> roles;
  private boolean isClusterTestingEnabled = false;

  @Inject
  public void configure(
      Properties props,
      @Self Set<NodeRole> roles
  )
  {
    this.isClusterTestingEnabled = Boolean.parseBoolean(
        props.getProperty("robux.unsafe.cluster.testing", "false")
    );
    this.roles = roles;
  }

  @Override
  public void configure(Binder binder)
  {
    if (isClusterTestingEnabled) {
      log.warn(
          "Running service in cluster testing mode. This is an unsafe test-only"
          + " mode and must never be used in a production cluster."
          + " Set property[robux.unsafe.cluster.testing=false] to disable testing mode."
      );
      bindDependenciesForClusterTestingMode(binder);
    } else {
      log.warn("Cluster testing is disabled. Set property[robux.unsafe.cluster.testing=true] to enable it.");
    }
  }

  private void bindDependenciesForClusterTestingMode(Binder binder)
  {
    if (roles.equals(Set.of(NodeRole.PEON))) {
      // Bind cluster testing config
      binder.bind(ClusterTestingTaskConfig.class)
            .toProvider(TestConfigProvider.class)
            .in(LazySingleton.class);

      // Bind faulty clients for Coordinator, Overlord and task actions
      binder.bind(CoordinatorClient.class)
            .to(FaultyCoordinatorClient.class)
            .in(LazySingleton.class);
      binder.bind(OverlordClient.class)
            .to(FaultyOverlordClient.class)
            .in(LazySingleton.class);
      binder.bind(RemoteTaskActionClientFactory.class)
            .to(FaultyRemoteTaskActionClientFactory.class)
            .in(LazySingleton.class);
    } else if (roles.contains(NodeRole.OVERLORD)) {
      // If this is the Overlord, bind a faulty storage coordinator
      log.warn("Running Overlord in cluster testing mode.");
      binder.bind(GlobalTaskLockbox.class)
            .to(FaultyTaskLockbox.class)
            .in(LazySingleton.class);
    }
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return List.of(
        new SimpleModule(getClass().getSimpleName())
            .registerSubtypes(new NamedType(FaultyLagAggregator.class, "faulty"))
    );
  }

  private static class TestConfigProvider implements Provider<ClusterTestingTaskConfig>
  {
    private final Task task;
    private final ObjectMapper mapper;

    @Inject
    public TestConfigProvider(Task task, ObjectMapper mapper)
    {
      this.task = task;
      this.mapper = mapper;
    }

    @Override
    public ClusterTestingTaskConfig get()
    {
      try {
        final ClusterTestingTaskConfig testingConfig = ClusterTestingTaskConfig.forTask(task, mapper);
        log.warn("Running task in cluster testing mode with config[%s].", testingConfig);

        return testingConfig;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

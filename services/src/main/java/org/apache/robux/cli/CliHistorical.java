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

package org.apache.robux.cli;

import com.github.rvesse.airline.annotations.Command;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.curator.ZkEnablementConfig;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.CacheModule;
import org.apache.robux.guice.RobuxProcessingModule;
import org.apache.robux.guice.HistoricalServiceModule;
import org.apache.robux.guice.Jerseys;
import org.apache.robux.guice.JoinableFactoryModule;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.guice.QueryRunnerFactoryModule;
import org.apache.robux.guice.QueryableModule;
import org.apache.robux.guice.SegmentWranglerModule;
import org.apache.robux.guice.ServerTypeConfig;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.lookup.LookupModule;
import org.apache.robux.server.QueryResource;
import org.apache.robux.server.ResponseContextConfig;
import org.apache.robux.server.SegmentManager;
import org.apache.robux.server.coordination.SegmentBootstrapper;
import org.apache.robux.server.coordination.ServerManager;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordination.ZkCoordinator;
import org.apache.robux.server.http.HistoricalResource;
import org.apache.robux.server.http.SegmentListerResource;
import org.apache.robux.server.http.SelfDiscoveryResource;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.apache.robux.server.metrics.QueryCountStatsProvider;
import org.apache.robux.storage.local.LocalTmpStorageConfig;
import org.apache.robux.timeline.PruneLastCompactionState;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Properties;
import java.util.Set;

@Command(
    name = "historical",
    description = "Runs a Historical node, see https://robux.apache.org/docs/latest/Historical.html for a description"
)
public class CliHistorical extends ServerRunnable
{
  private static final Logger log = new Logger(CliHistorical.class);

  private boolean isZkEnabled = true;

  public CliHistorical()
  {
    super(log);
  }

  @Inject
  public void configure(Properties properties)
  {
    isZkEnabled = ZkEnablementConfig.isEnabled(properties);
  }

  @Override
  protected Set<NodeRole> getNodeRoles(Properties properties)
  {
    return ImmutableSet.of(NodeRole.HISTORICAL);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new RobuxProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new SegmentWranglerModule(),
        new JoinableFactoryModule(),
        new HistoricalServiceModule(),
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("robux/historical");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8083);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8283);
          binder.bindConstant().annotatedWith(PruneLastCompactionState.class).to(true);
          binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(true));

          // register Server before binding ZkCoordinator to ensure HTTP endpoints are available immediately
          LifecycleModule.register(binder, Server.class);
          binder.bind(ServerManager.class).in(LazySingleton.class);
          binder.bind(SegmentManager.class).in(LazySingleton.class);
          binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
          bindQuerySegmentWalker(binder);

          binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.HISTORICAL));
          binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);
          binder.bind(QueryCountStatsProvider.class).to(QueryResource.class);
          Jerseys.addResource(binder, QueryResource.class);
          Jerseys.addResource(binder, SegmentListerResource.class);
          Jerseys.addResource(binder, HistoricalResource.class);
          LifecycleModule.register(binder, QueryResource.class);

          if (isZkEnabled) {
            LifecycleModule.register(binder, ZkCoordinator.class);
          }
          LifecycleModule.register(binder, SegmentBootstrapper.class);

          JsonConfigProvider.bind(binder, "robux.historical.cache", CacheConfig.class);
          binder.install(new CacheModule());

          bindAnnouncer(
              binder,
              DiscoverySideEffectsProvider.create()
          );

          Jerseys.addResource(binder, SelfDiscoveryResource.class);
          LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));

          binder.bind(LocalTmpStorageConfig.class)
                .toProvider(new LocalTmpStorageConfig.DefaultLocalTmpStorageConfigProvider("historical"))
                .in(LazySingleton.class);
        },
        new LookupModule()
    );
  }

  /**
   * This method is visible for testing query retry on missing segments. See {@link CliHistoricalForQueryErrorTest}.
   */
  @VisibleForTesting
  public void bindQuerySegmentWalker(Binder binder)
  {
    binder.bind(QuerySegmentWalker.class).to(ServerManager.class).in(LazySingleton.class);
  }
}

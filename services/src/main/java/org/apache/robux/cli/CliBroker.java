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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.robux.client.BrokerSegmentWatcherConfig;
import org.apache.robux.client.BrokerServerView;
import org.apache.robux.client.BrokerViewOfCoordinatorConfig;
import org.apache.robux.client.CachingClusteredClient;
import org.apache.robux.client.DirectRobuxClientFactory;
import org.apache.robux.client.HttpServerInventoryViewResource;
import org.apache.robux.client.InternalQueryConfig;
import org.apache.robux.client.QueryableRobuxServer;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.client.selector.CustomTierSelectorStrategyConfig;
import org.apache.robux.client.selector.PreferredTierSelectorStrategyConfig;
import org.apache.robux.client.selector.ServerSelectorStrategy;
import org.apache.robux.client.selector.TierSelectorStrategy;
import org.apache.robux.curator.ZkEnablementConfig;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.BrokerProcessingModule;
import org.apache.robux.guice.BrokerServiceModule;
import org.apache.robux.guice.CacheModule;
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
import org.apache.robux.query.RetryQueryRunnerConfig;
import org.apache.robux.query.lookup.LookupModule;
import org.apache.robux.server.BrokerDynamicConfigResource;
import org.apache.robux.server.BrokerQueryResource;
import org.apache.robux.server.ClientInfoResource;
import org.apache.robux.server.ClientQuerySegmentWalker;
import org.apache.robux.server.ResponseContextConfig;
import org.apache.robux.server.SegmentManager;
import org.apache.robux.server.SubqueryGuardrailHelper;
import org.apache.robux.server.SubqueryGuardrailHelperProvider;
import org.apache.robux.server.coordination.SegmentBootstrapper;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordination.ZkCoordinator;
import org.apache.robux.server.http.BrokerResource;
import org.apache.robux.server.http.HistoricalResource;
import org.apache.robux.server.http.SegmentListerResource;
import org.apache.robux.server.http.SelfDiscoveryResource;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.apache.robux.server.metrics.QueryCountStatsProvider;
import org.apache.robux.server.metrics.SubqueryCountStatsProvider;
import org.apache.robux.server.router.TieredBrokerConfig;
import org.apache.robux.sql.calcite.schema.MetadataSegmentView;
import org.apache.robux.sql.guice.SqlModule;
import org.apache.robux.storage.local.LocalTmpStorageConfig;
import org.apache.robux.timeline.PruneLoadSpec;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Properties;
import java.util.Set;

@Command(
    name = "broker",
    description = "Runs a broker node, see https://robux.apache.org/docs/latest/Broker.html for a description"
)
public class CliBroker extends ServerRunnable
{
  private static final Logger log = new Logger(CliBroker.class);

  private boolean isZkEnabled = true;

  public CliBroker()
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
    return ImmutableSet.of(NodeRole.BROKER);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new BrokerProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new SegmentWranglerModule(),
        new JoinableFactoryModule(),
        new BrokerServiceModule(),
        binder -> {
          validateCentralizedDatasourceSchemaConfig(getProperties());

          binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
              TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME
          );
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8082);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8282);
          binder.bindConstant().annotatedWith(PruneLoadSpec.class).to(true);
          binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(false));

          binder.bind(CachingClusteredClient.class).in(LazySingleton.class);
          LifecycleModule.register(binder, BrokerServerView.class);
          LifecycleModule.register(binder, MetadataSegmentView.class);
          binder.bind(TimelineServerView.class).to(BrokerServerView.class).in(LazySingleton.class);
          binder.bind(QueryableRobuxServer.Maker.class).to(DirectRobuxClientFactory.class).in(LazySingleton.class);

          JsonConfigProvider.bind(binder, "robux.broker.cache", CacheConfig.class);
          binder.install(new CacheModule());

          JsonConfigProvider.bind(binder, "robux.broker.select", TierSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "robux.broker.select.tier.custom", CustomTierSelectorStrategyConfig.class);
          JsonConfigProvider.bind(binder, "robux.broker.select.tier.preferred", PreferredTierSelectorStrategyConfig.class);
          JsonConfigProvider.bind(binder, "robux.broker.balancer", ServerSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "robux.broker.retryPolicy", RetryQueryRunnerConfig.class);
          JsonConfigProvider.bind(binder, "robux.broker.segment", BrokerSegmentWatcherConfig.class);
          JsonConfigProvider.bind(binder, "robux.broker.internal.query.config", InternalQueryConfig.class);

          binder.bind(QuerySegmentWalker.class).to(ClientQuerySegmentWalker.class).in(LazySingleton.class);

          binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);

          binder.bind(BrokerQueryResource.class).in(LazySingleton.class);
          Jerseys.addResource(binder, BrokerQueryResource.class);
          binder.bind(SubqueryGuardrailHelper.class).toProvider(SubqueryGuardrailHelperProvider.class);
          binder.bind(QueryCountStatsProvider.class).to(BrokerQueryResource.class).in(LazySingleton.class);
          binder.bind(SubqueryCountStatsProvider.class).toInstance(new SubqueryCountStatsProvider());
          Jerseys.addResource(binder, BrokerResource.class);
          Jerseys.addResource(binder, ClientInfoResource.class);
          Jerseys.addResource(binder, BrokerDynamicConfigResource.class);

          LifecycleModule.register(binder, BrokerQueryResource.class);

          Jerseys.addResource(binder, HttpServerInventoryViewResource.class);

          LifecycleModule.register(binder, Server.class);
          binder.bind(SegmentManager.class).in(LazySingleton.class);
          binder.bind(BrokerViewOfCoordinatorConfig.class).in(ManageLifecycle.class);
          binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
          binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.BROKER));
          Jerseys.addResource(binder, HistoricalResource.class);
          Jerseys.addResource(binder, SegmentListerResource.class);

          if (isZkEnabled) {
            LifecycleModule.register(binder, ZkCoordinator.class);
          }
          LifecycleModule.register(binder, SegmentBootstrapper.class);

          bindAnnouncer(
              binder,
              DiscoverySideEffectsProvider.withLegacyAnnouncer()
          );

          Jerseys.addResource(binder, SelfDiscoveryResource.class);
          LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));

          binder.bind(LocalTmpStorageConfig.class)
                .toProvider(new LocalTmpStorageConfig.DefaultLocalTmpStorageConfigProvider("broker"))
                .in(LazySingleton.class);
        },
        new LookupModule(),
        new SqlModule()
    );
  }
}

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

package org.apache.robux.quidem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import org.apache.robux.cli.CliBroker;
import org.apache.robux.cli.QueryJettyServerInitializer;
import org.apache.robux.client.BrokerSegmentWatcherConfig;
import org.apache.robux.client.DirectRobuxClientFactory;
import org.apache.robux.client.InternalQueryConfig;
import org.apache.robux.client.QueryableRobuxServer;
import org.apache.robux.client.selector.CustomTierSelectorStrategyConfig;
import org.apache.robux.client.selector.ServerSelectorStrategy;
import org.apache.robux.client.selector.TierSelectorStrategy;
import org.apache.robux.curator.CuratorModule;
import org.apache.robux.curator.discovery.DiscoveryModule;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.guice.AnnouncerModule;
import org.apache.robux.guice.BrokerServiceModule;
import org.apache.robux.guice.CoordinatorDiscoveryModule;
import org.apache.robux.guice.ExpressionModule;
import org.apache.robux.guice.ExtensionsModule;
import org.apache.robux.guice.JacksonConfigManagerModule;
import org.apache.robux.guice.JavaScriptModule;
import org.apache.robux.guice.Jerseys;
import org.apache.robux.guice.JoinableFactoryModule;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.LocalDataStorageRobuxModule;
import org.apache.robux.guice.MetadataConfigModule;
import org.apache.robux.guice.SegmentWranglerModule;
import org.apache.robux.guice.ServerViewModule;
import org.apache.robux.guice.StartupLoggingModule;
import org.apache.robux.guice.annotations.Client;
import org.apache.robux.guice.annotations.EscalatedClient;
import org.apache.robux.guice.http.HttpClientModule;
import org.apache.robux.guice.security.AuthenticatorModule;
import org.apache.robux.guice.security.AuthorizerModule;
import org.apache.robux.guice.security.RobuxAuthModule;
import org.apache.robux.initialization.CoreInjectorBuilder;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.initialization.Log4jShutterDownerModule;
import org.apache.robux.initialization.ServerInjectorBuilder;
import org.apache.robux.initialization.TombstoneDataStorageModule;
import org.apache.robux.metadata.storage.derby.DerbyMetadataStorageRobuxModule;
import org.apache.robux.query.RetryQueryRunnerConfig;
import org.apache.robux.rpc.guice.ServiceClientModule;
import org.apache.robux.segment.writeout.SegmentWriteOutMediumModule;
import org.apache.robux.server.BrokerQueryResource;
import org.apache.robux.server.ClientInfoResource;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.http.BrokerResource;
import org.apache.robux.server.http.SelfDiscoveryResource;
import org.apache.robux.server.initialization.ExternalStorageAccessSecurityModule;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.apache.robux.server.initialization.jetty.JettyServerModule;
import org.apache.robux.server.metrics.QueryCountStatsProvider;
import org.apache.robux.server.metrics.SubqueryCountStatsProvider;
import org.apache.robux.server.router.TieredBrokerConfig;
import org.apache.robux.server.security.TLSCertificateCheckerModule;
import org.apache.robux.sql.calcite.schema.BrokerSegmentMetadataCache;
import org.apache.robux.sql.calcite.schema.RobuxSchemaName;
import org.apache.robux.sql.calcite.util.CalciteTests;
import org.apache.robux.sql.calcite.util.RobuxModuleCollection;
import org.apache.robux.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.robux.sql.calcite.util.SqlTestFramework.QueryComponentSupplierDelegate;
import org.apache.robux.storage.StorageConnectorModule;
import org.apache.robux.timeline.PruneLoadSpec;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Properties;

/**
 * A wrapper class to expose a {@link QueryComponentSupplier} as a Broker service.
 */
public class ExposedAsBrokerQueryComponentSupplierWrapper extends QueryComponentSupplierDelegate
{
  public ExposedAsBrokerQueryComponentSupplierWrapper(QueryComponentSupplier delegate)
  {
    super(delegate);
  }

  @Override
  public void gatherProperties(Properties properties)
  {
    properties.put("robux.enableTlsPort", "false");
    properties.put("robux.zk.service.enabled", "false");
    properties.put("robux.plaintextPort", "12345");
    properties.put("robux.host", "localhost");
    properties.put("robux.broker.segment.awaitInitializationOnStart", "false");
  }

  @Override
  public RobuxModule getCoreModule()
  {
    Builder<Module> modules = ImmutableList.builder();
    modules.add(super.getCoreModule());
    modules.addAll(forServerModules());

    modules.addAll(brokerModules());
    modules.add(new QuidemCaptureModule());

    return RobuxModuleCollection.of(modules.build());

  }

  @Override
  public RobuxModule getOverrideModule()
  {
    return RobuxModuleCollection.of(
        super.getOverrideModule(),
        new BrokerTestModule()
    );
  }

  public static class BrokerTestModule extends AbstractModule
  {
    @Override
    protected void configure()
    {
    }

    @Provides
    @LazySingleton
    public BrokerSegmentMetadataCache provideCache()
    {
      return null;
    }

    @Provides
    @LazySingleton
    RobuxNodeDiscoveryProvider getRobuxNodeDiscoveryProvider()
    {
      final RobuxNode coordinatorNode = CalciteTests.mockCoordinatorNode();
      return CalciteTests.mockRobuxNodeDiscoveryProvider(coordinatorNode);
    }
  }

  /**
   * Closely related to {@link CoreInjectorBuilder#forServer()}
   */
  private List<Module> forServerModules()
  {
    return ImmutableList.of(
        new Log4jShutterDownerModule(),
        new ExtensionsModule.SecondaryModule(),
        new RobuxAuthModule(),
        new TLSCertificateCheckerModule(),
        HttpClientModule.global(),
        HttpClientModule.escalatedGlobal(),
        new HttpClientModule("robux.broker.http", Client.class, true),
        new HttpClientModule("robux.broker.http", EscalatedClient.class, true),
        new CuratorModule(),
        new AnnouncerModule(),
        new SegmentWriteOutMediumModule(),
        new JettyServerModule(),
        new ExpressionModule(),
        new DiscoveryModule(),
        new ServerViewModule(),
        new MetadataConfigModule(),
        new DerbyMetadataStorageRobuxModule(),
        new JacksonConfigManagerModule(),
        new CoordinatorDiscoveryModule(),
        new LocalDataStorageRobuxModule(),
        new TombstoneDataStorageModule(),
        new JavaScriptModule(),
        new AuthenticatorModule(),
        new AuthorizerModule(),
        new StartupLoggingModule(),
        new ExternalStorageAccessSecurityModule(),
        new ServiceClientModule(),
        new StorageConnectorModule(),
        ServerInjectorBuilder.registerNodeRoleModule(ImmutableSet.of())
    );
  }

  /**
   * Closely related to {@link CliBroker#getModules}.
   */
  static List<? extends Module> brokerModules()
  {
    return ImmutableList.of(
        new SegmentWranglerModule(),
        new JoinableFactoryModule(),
        new BrokerServiceModule(),
        binder -> {

          binder.bind(QueryableRobuxServer.Maker.class).to(DirectRobuxClientFactory.class).in(LazySingleton.class);
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to(
              TieredBrokerConfig.DEFAULT_BROKER_SERVICE_NAME
          );
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8082);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8282);
          binder.bindConstant().annotatedWith(PruneLoadSpec.class).to(true);

          JsonConfigProvider.bind(binder, "robux.broker.select", TierSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "robux.broker.select.tier.custom", CustomTierSelectorStrategyConfig.class);
          JsonConfigProvider.bind(binder, "robux.broker.balancer", ServerSelectorStrategy.class);
          JsonConfigProvider.bind(binder, "robux.broker.retryPolicy", RetryQueryRunnerConfig.class);
          JsonConfigProvider.bind(binder, "robux.broker.segment", BrokerSegmentWatcherConfig.class);
          JsonConfigProvider.bind(binder, "robux.broker.internal.query.config", InternalQueryConfig.class);
          binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class).in(LazySingleton.class);

          binder.bind(BrokerQueryResource.class).in(LazySingleton.class);
          Jerseys.addResource(binder, BrokerQueryResource.class);
          binder.bind(QueryCountStatsProvider.class).to(BrokerQueryResource.class).in(LazySingleton.class);
          binder.bind(SubqueryCountStatsProvider.class).toInstance(new SubqueryCountStatsProvider());
          Jerseys.addResource(binder, BrokerResource.class);
          Jerseys.addResource(binder, ClientInfoResource.class);

          LifecycleModule.register(binder, BrokerQueryResource.class);

          LifecycleModule.register(binder, Server.class);

          binder.bind(String.class)
              .annotatedWith(RobuxSchemaName.class)
              .toInstance(CalciteTests.ROBUX_SCHEMA_NAME);

          Jerseys.addResource(binder, SelfDiscoveryResource.class);
          LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));
        }
    );
  }
}

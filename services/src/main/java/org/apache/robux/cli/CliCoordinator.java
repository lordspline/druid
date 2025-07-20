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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rvesse.airline.annotations.Command;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import org.apache.robux.client.CoordinatorSegmentWatcherConfig;
import org.apache.robux.client.CoordinatorServerView;
import org.apache.robux.client.DirectRobuxClientFactory;
import org.apache.robux.client.HttpServerInventoryViewResource;
import org.apache.robux.client.coordinator.Coordinator;
import org.apache.robux.discovery.RobuxLeaderSelector;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.Jerseys;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.JsonConfigurator;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.guice.MetadataConfigModule;
import org.apache.robux.guice.MetadataManagerModule;
import org.apache.robux.guice.QueryableModule;
import org.apache.robux.guice.SegmentSchemaCacheModule;
import org.apache.robux.guice.SupervisorCleanupModule;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.http.JettyHttpClientModule;
import org.apache.robux.java.util.common.IAE;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.java.util.common.concurrent.ExecutorServices;
import org.apache.robux.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.robux.java.util.common.lifecycle.Lifecycle;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.metadata.MetadataStorage;
import org.apache.robux.metadata.MetadataStorageProvider;
import org.apache.robux.query.lookup.LookupSerdeModule;
import org.apache.robux.segment.metadata.CoordinatorSegmentMetadataCache;
import org.apache.robux.segment.metadata.SegmentMetadataCacheConfig;
import org.apache.robux.server.compaction.CompactionStatusTracker;
import org.apache.robux.server.coordinator.CloneStatusManager;
import org.apache.robux.server.coordinator.CoordinatorConfigManager;
import org.apache.robux.server.coordinator.RobuxCoordinator;
import org.apache.robux.server.coordinator.balancer.BalancerStrategyFactory;
import org.apache.robux.server.coordinator.config.CoordinatorKillConfigs;
import org.apache.robux.server.coordinator.config.CoordinatorPeriodConfig;
import org.apache.robux.server.coordinator.config.CoordinatorRunConfig;
import org.apache.robux.server.coordinator.config.RobuxCoordinatorConfig;
import org.apache.robux.server.coordinator.config.HttpLoadQueuePeonConfig;
import org.apache.robux.server.coordinator.duty.CoordinatorCustomDuty;
import org.apache.robux.server.coordinator.duty.CoordinatorCustomDutyGroup;
import org.apache.robux.server.coordinator.duty.CoordinatorCustomDutyGroups;
import org.apache.robux.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.robux.server.http.ClusterResource;
import org.apache.robux.server.http.CoordinatorCompactionConfigsResource;
import org.apache.robux.server.http.CoordinatorCompactionResource;
import org.apache.robux.server.http.CoordinatorDynamicConfigSyncer;
import org.apache.robux.server.http.CoordinatorDynamicConfigsResource;
import org.apache.robux.server.http.CoordinatorRedirectInfo;
import org.apache.robux.server.http.CoordinatorResource;
import org.apache.robux.server.http.DataSourcesResource;
import org.apache.robux.server.http.IntervalsResource;
import org.apache.robux.server.http.LookupCoordinatorResource;
import org.apache.robux.server.http.MetadataResource;
import org.apache.robux.server.http.RedirectFilter;
import org.apache.robux.server.http.RedirectInfo;
import org.apache.robux.server.http.RulesResource;
import org.apache.robux.server.http.SelfDiscoveryResource;
import org.apache.robux.server.http.ServersResource;
import org.apache.robux.server.http.TiersResource;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.apache.robux.server.lookup.cache.LookupCoordinatorManager;
import org.apache.robux.server.lookup.cache.LookupCoordinatorManagerConfig;
import org.apache.robux.server.metrics.ServiceStatusMonitor;
import org.apache.robux.server.router.TieredBrokerConfig;
import org.apache.robux.storage.local.LocalTmpStorageConfig;
import org.eclipse.jetty.server.Server;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 *
 */
@Command(
    name = "coordinator",
    description = "Runs the Coordinator, see https://robux.apache.org/docs/latest/Coordinator.html for a description."
)
public class CliCoordinator extends ServerRunnable
{
  private static final Logger log = new Logger(CliCoordinator.class);
  private static final String AS_OVERLORD_PROPERTY = "robux.coordinator.asOverlord.enabled";
  private Properties properties;
  private boolean beOverlord;
  private boolean isSegmentSchemaCacheEnabled;

  public CliCoordinator()
  {
    super(log);
  }

  @Inject
  public void configure(Properties properties)
  {
    this.properties = properties;
    beOverlord = isOverlord(properties);
    isSegmentSchemaCacheEnabled = MetadataConfigModule.isSegmentSchemaCacheEnabled(properties);

    if (beOverlord) {
      log.info("Coordinator is configured to act as Overlord as well (%s = true).", AS_OVERLORD_PROPERTY);
    }
  }

  @Override
  protected Set<NodeRole> getNodeRoles(Properties properties)
  {
    return isOverlord(properties)
           ? ImmutableSet.of(NodeRole.COORDINATOR, NodeRole.OVERLORD)
           : ImmutableSet.of(NodeRole.COORDINATOR);
  }


  @Override
  protected List<? extends Module> getModules()
  {
    List<Module> modules = new ArrayList<>();

    modules.add(JettyHttpClientModule.global());
    modules.add(new MetadataManagerModule());

    if (isSegmentSchemaCacheEnabled) {
      validateCentralizedDatasourceSchemaConfig(properties);
      modules.add(new SegmentSchemaCacheModule());
      modules.add(new QueryableModule());
    }

    modules.add(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bindConstant()
                  .annotatedWith(Names.named("serviceName"))
                  .to(TieredBrokerConfig.DEFAULT_COORDINATOR_SERVICE_NAME);
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8081);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8281);

            binder.bind(MetadataStorage.class).toProvider(MetadataStorageProvider.class);

            JsonConfigProvider.bind(binder, "robux.manager.lookups", LookupCoordinatorManagerConfig.class);
            JsonConfigProvider.bind(binder, "robux.coordinator", CoordinatorRunConfig.class);
            JsonConfigProvider.bind(binder, "robux.coordinator.kill", CoordinatorKillConfigs.class);
            JsonConfigProvider.bind(binder, "robux.coordinator.period", CoordinatorPeriodConfig.class);
            JsonConfigProvider.bind(binder, "robux.coordinator.loadqueuepeon.http", HttpLoadQueuePeonConfig.class);
            JsonConfigProvider.bind(binder, "robux.coordinator.balancer", BalancerStrategyFactory.class);
            JsonConfigProvider.bind(binder, "robux.coordinator.segment", CoordinatorSegmentWatcherConfig.class);
            JsonConfigProvider.bind(binder, "robux.coordinator.segmentMetadataCache", SegmentMetadataCacheConfig.class);
            binder.bind(RobuxCoordinatorConfig.class);

            binder.bind(RedirectFilter.class).in(LazySingleton.class);
            binder.bind(CoordinatorDynamicConfigSyncer.class).in(ManageLifecycle.class);
            if (beOverlord) {
              binder.bind(RedirectInfo.class).to(CoordinatorOverlordRedirectInfo.class).in(LazySingleton.class);
            } else {
              binder.bind(RedirectInfo.class).to(CoordinatorRedirectInfo.class).in(LazySingleton.class);
            }

            LifecycleModule.register(binder, CoordinatorServerView.class);

            if (!isSegmentSchemaCacheEnabled) {
              binder.bind(CoordinatorSegmentMetadataCache.class).toProvider(Providers.of(null));
              binder.bind(DirectRobuxClientFactory.class).toProvider(Providers.of(null));
            }

            binder.bind(LookupCoordinatorManager.class).in(LazySingleton.class);
            binder.bind(CloneStatusManager.class).in(LazySingleton.class);

            binder.bind(RobuxCoordinator.class);
            binder.bind(CompactionStatusTracker.class).in(LazySingleton.class);

            LifecycleModule.register(binder, MetadataStorage.class);
            LifecycleModule.register(binder, RobuxCoordinator.class);

            binder.bind(JettyServerInitializer.class)
                  .to(CoordinatorJettyServerInitializer.class);

            Jerseys.addResource(binder, CoordinatorResource.class);
            Jerseys.addResource(binder, CoordinatorCompactionResource.class);
            Jerseys.addResource(binder, CoordinatorDynamicConfigsResource.class);
            Jerseys.addResource(binder, CoordinatorCompactionConfigsResource.class);
            Jerseys.addResource(binder, TiersResource.class);
            Jerseys.addResource(binder, RulesResource.class);
            Jerseys.addResource(binder, ServersResource.class);
            Jerseys.addResource(binder, DataSourcesResource.class);
            Jerseys.addResource(binder, MetadataResource.class);
            Jerseys.addResource(binder, IntervalsResource.class);
            Jerseys.addResource(binder, LookupCoordinatorResource.class);
            Jerseys.addResource(binder, ClusterResource.class);
            Jerseys.addResource(binder, HttpServerInventoryViewResource.class);

            LifecycleModule.register(binder, Server.class);
            LifecycleModule.register(binder, DataSourcesResource.class);

            bindAnnouncer(
                binder,
                Coordinator.class,
                DiscoverySideEffectsProvider.create()
            );

            Jerseys.addResource(binder, SelfDiscoveryResource.class);
            LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));

            if (!beOverlord) {
              // Bind HeartbeatSupplier only when the service operates independently of Overlord.
              binder.bind(new TypeLiteral<Supplier<Map<String, Object>>>() {})
                  .annotatedWith(Names.named(ServiceStatusMonitor.HEARTBEAT_TAGS_BINDING))
                  .toProvider(HeartbeatSupplier.class);
              binder.bind(LocalTmpStorageConfig.class)
                    .toProvider(new LocalTmpStorageConfig.DefaultLocalTmpStorageConfigProvider("coordinator"))
                    .in(LazySingleton.class);
            }

            binder.bind(CoordinatorCustomDutyGroups.class)
                  .toProvider(new CoordinatorCustomDutyGroupsProvider())
                  .in(LazySingleton.class);
          }

          @Provides
          @LazySingleton
          public LoadQueueTaskMaster getLoadQueueTaskMaster(
              ObjectMapper jsonMapper,
              ScheduledExecutorFactory factory,
              RobuxCoordinatorConfig config,
              @EscalatedGlobal HttpClient httpClient,
              Lifecycle lifecycle,
              CoordinatorConfigManager coordinatorConfigManager
          )
          {
            final ExecutorService callBackExec = Execs.singleThreaded("LoadQueuePeon-callbackexec--%d");
            ExecutorServices.manageLifecycle(lifecycle, callBackExec);
            return new LoadQueueTaskMaster(
                jsonMapper,
                factory.create(1, "Master-PeonExec--%d"),
                callBackExec,
                config.getHttpLoadQueuePeonConfig(),
                httpClient,
                coordinatorConfigManager::getCurrentDynamicConfig
            );
          }
        }
    );

    if (beOverlord) {
      CliOverlord cliOverlord = new CliOverlord();
      cliOverlord.configure(properties);
      modules.addAll(cliOverlord.getModules(false));
    } else {
      // Only add LookupSerdeModule if !beOverlord, since CliOverlord includes it, and having two copies causes
      // the injector to get confused due to having multiple bindings for the same classes.
      modules.add(new LookupSerdeModule());
      modules.add(new SupervisorCleanupModule());
    }

    return modules;
  }

  public static boolean isOverlord(Properties properties)
  {
    return Boolean.parseBoolean(properties.getProperty(AS_OVERLORD_PROPERTY));
  }


  private static class CoordinatorCustomDutyGroupsProvider implements Provider<CoordinatorCustomDutyGroups>
  {
    private Properties props;
    private JsonConfigurator configurator;
    private ObjectMapper jsonMapper;

    @Inject
    public void inject(Properties props, JsonConfigurator configurator, ObjectMapper jsonMapper)
    {
      this.props = props;
      this.configurator = configurator;
      this.jsonMapper = jsonMapper;
    }

    @Override
    public CoordinatorCustomDutyGroups get()
    {
      try {
        Set<CoordinatorCustomDutyGroup> coordinatorCustomDutyGroups = new HashSet<>();
        if (Strings.isNullOrEmpty(props.getProperty("robux.coordinator.dutyGroups"))) {
          return new CoordinatorCustomDutyGroups(coordinatorCustomDutyGroups);
        }
        List<String> coordinatorCustomDutyGroupNames = jsonMapper.readValue(props.getProperty(
            "robux.coordinator.dutyGroups"), new TypeReference<>() {});
        for (String coordinatorCustomDutyGroupName : coordinatorCustomDutyGroupNames) {
          String dutyListProperty = StringUtils.format("robux.coordinator.%s.duties", coordinatorCustomDutyGroupName);
          if (Strings.isNullOrEmpty(props.getProperty(dutyListProperty))) {
            throw new IAE("Coordinator custom duty group given without any duty for group %s", coordinatorCustomDutyGroupName);
          }
          List<String> dutyForGroup = jsonMapper.readValue(props.getProperty(dutyListProperty), new TypeReference<>() {});
          List<CoordinatorCustomDuty> coordinatorCustomDuties = new ArrayList<>();
          for (String dutyName : dutyForGroup) {
            final String dutyPropertyBase = StringUtils.format(
                "robux.coordinator.%s.duty.%s",
                coordinatorCustomDutyGroupName,
                dutyName
            );
            final JsonConfigProvider<CoordinatorCustomDuty> coordinatorCustomDutyProvider = JsonConfigProvider.of(
                dutyPropertyBase,
                CoordinatorCustomDuty.class
            );

            String typeProperty = StringUtils.format("%s.type", dutyPropertyBase);
            Properties adjustedProps = new Properties(props);
            if (adjustedProps.containsKey(typeProperty)) {
              throw new IAE("'type' property [%s] is reserved.", typeProperty);
            } else {
              adjustedProps.put(typeProperty, dutyName);
            }
            coordinatorCustomDutyProvider.inject(adjustedProps, configurator);
            CoordinatorCustomDuty coordinatorCustomDuty = coordinatorCustomDutyProvider.get();
            if (coordinatorCustomDuty == null) {
              throw new ISE("Could not create CoordinatorCustomDuty with name: %s for group: %s", dutyName, coordinatorCustomDutyGroupName);
            }
            coordinatorCustomDuties.add(coordinatorCustomDuty);
          }
          String groupPeriodPropKey = StringUtils.format("robux.coordinator.%s.period", coordinatorCustomDutyGroupName);
          if (Strings.isNullOrEmpty(props.getProperty(groupPeriodPropKey))) {
            throw new IAE("Run period for coordinator custom duty group must be set for group %s", coordinatorCustomDutyGroupName);
          }
          Duration groupPeriod = new Duration(props.getProperty(groupPeriodPropKey));
          coordinatorCustomDutyGroups.add(new CoordinatorCustomDutyGroup(coordinatorCustomDutyGroupName, groupPeriod, coordinatorCustomDuties));
        }
        return new CoordinatorCustomDutyGroups(coordinatorCustomDutyGroups);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class HeartbeatSupplier implements Provider<Supplier<Map<String, Object>>>
  {
    private final RobuxLeaderSelector leaderSelector;

    @Inject
    public HeartbeatSupplier(@Coordinator RobuxLeaderSelector leaderSelector)
    {
      this.leaderSelector = leaderSelector;
    }

    @Override
    public Supplier<Map<String, Object>> get()
    {
      return () -> {
        Map<String, Object> heartbeatTags = new HashMap<>();
        heartbeatTags.put("leader", leaderSelector.isLeader() ? 1 : 0);

        return heartbeatTags;
      };
    }
  }

}

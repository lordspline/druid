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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import org.apache.robux.curator.ZkEnablementConfig;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.discovery.WorkerNodeService;
import org.apache.robux.guice.IndexingServiceInputSourceModule;
import org.apache.robux.guice.IndexingServiceModuleHelper;
import org.apache.robux.guice.IndexingServiceTaskLogsModule;
import org.apache.robux.guice.IndexingServiceTuningConfigModule;
import org.apache.robux.guice.Jerseys;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.guice.MiddleManagerServiceModule;
import org.apache.robux.guice.PolyBind;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.indexer.HadoopIndexTaskModule;
import org.apache.robux.indexing.common.RetryPolicyFactory;
import org.apache.robux.indexing.common.TaskStorageDirTracker;
import org.apache.robux.indexing.common.config.TaskConfig;
import org.apache.robux.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.robux.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClientProvider;
import org.apache.robux.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.robux.indexing.overlord.ForkingTaskRunner;
import org.apache.robux.indexing.overlord.TaskRunner;
import org.apache.robux.indexing.worker.Worker;
import org.apache.robux.indexing.worker.WorkerCuratorCoordinator;
import org.apache.robux.indexing.worker.WorkerTaskManager;
import org.apache.robux.indexing.worker.WorkerTaskMonitor;
import org.apache.robux.indexing.worker.config.WorkerConfig;
import org.apache.robux.indexing.worker.http.TaskManagementResource;
import org.apache.robux.indexing.worker.http.WorkerResource;
import org.apache.robux.indexing.worker.shuffle.DeepStorageIntermediaryDataManager;
import org.apache.robux.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.robux.indexing.worker.shuffle.LocalIntermediaryDataManager;
import org.apache.robux.indexing.worker.shuffle.ShuffleModule;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.metadata.input.InputSourceModule;
import org.apache.robux.query.RobuxMetrics;
import org.apache.robux.query.lookup.LookupSerdeModule;
import org.apache.robux.segment.incremental.RowIngestionMetersFactory;
import org.apache.robux.segment.realtime.ChatHandlerProvider;
import org.apache.robux.segment.realtime.NoopChatHandlerProvider;
import org.apache.robux.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.robux.segment.realtime.appenderator.DummyForInjectionAppenderatorsManager;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.http.SelfDiscoveryResource;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.apache.robux.server.metrics.ServiceStatusMonitor;
import org.apache.robux.server.metrics.WorkerTaskCountStatsProvider;
import org.apache.robux.storage.local.LocalTmpStorageConfig;
import org.apache.robux.timeline.PruneLastCompactionState;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 *
 */
@Command(
    name = "middleManager",
    description = "Runs a Middle Manager, this is a \"task\" node used as part of the remote indexing service, see https://robux.apache.org/docs/latest/design/middlemanager.html for a description"
)
public class CliMiddleManager extends ServerRunnable
{
  private static final Logger log = new Logger(CliMiddleManager.class);

  private boolean isZkEnabled = true;

  public CliMiddleManager()
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
    return ImmutableSet.of(NodeRole.MIDDLE_MANAGER);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new MiddleManagerServiceModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            validateCentralizedDatasourceSchemaConfig(getProperties());

            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("robux/middlemanager");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8091);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8291);
            binder.bindConstant().annotatedWith(PruneLastCompactionState.class).to(true);

            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);

            JsonConfigProvider.bind(binder, "robux.indexer.task", TaskConfig.class);
            JsonConfigProvider.bind(binder, "robux.worker", WorkerConfig.class);
            binder.bind(RetryPolicyFactory.class).in(LazySingleton.class);

            binder.bind(TaskRunner.class).to(ForkingTaskRunner.class);
            binder.bind(ForkingTaskRunner.class).in(ManageLifecycle.class);
            binder.bind(WorkerTaskCountStatsProvider.class).to(ForkingTaskRunner.class);

            binder.bind(ParallelIndexSupervisorTaskClientProvider.class).toProvider(Providers.of(null));
            binder.bind(ShuffleClient.class).toProvider(Providers.of(null));
            binder.bind(ChatHandlerProvider.class).toProvider(Providers.of(new NoopChatHandlerProvider()));
            PolyBind.createChoice(
                binder,
                "robux.indexer.task.rowIngestionMeters.type",
                Key.get(RowIngestionMetersFactory.class),
                Key.get(DropwizardRowIngestionMetersFactory.class)
            );
            final MapBinder<String, RowIngestionMetersFactory> rowIngestionMetersHandlerProviderBinder =
                PolyBind.optionBinder(binder, Key.get(RowIngestionMetersFactory.class));
            rowIngestionMetersHandlerProviderBinder
                .addBinding("dropwizard")
                .to(DropwizardRowIngestionMetersFactory.class)
                .in(LazySingleton.class);
            binder.bind(DropwizardRowIngestionMetersFactory.class).in(LazySingleton.class);

            binder.install(makeWorkerManagementModule(isZkEnabled));

            binder.bind(JettyServerInitializer.class)
                  .to(MiddleManagerJettyServerInitializer.class)
                  .in(LazySingleton.class);

            binder.bind(AppenderatorsManager.class)
                  .to(DummyForInjectionAppenderatorsManager.class)
                  .in(LazySingleton.class);

            LifecycleModule.register(binder, Server.class);

            bindAnnouncer(
                binder,
                DiscoverySideEffectsProvider.create()
            );

            Jerseys.addResource(binder, SelfDiscoveryResource.class);
            LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));

            configureIntermediaryData(binder);

            binder.bind(LocalTmpStorageConfig.class)
                  .toProvider(new LocalTmpStorageConfig.DefaultLocalTmpStorageConfigProvider("middle-manager"))
                  .in(LazySingleton.class);
          }

          private void configureIntermediaryData(Binder binder)
          {
            PolyBind.createChoice(
                binder,
                "robux.processing.intermediaryData.storage.type",
                Key.get(IntermediaryDataManager.class),
                Key.get(LocalIntermediaryDataManager.class)
            );
            final MapBinder<String, IntermediaryDataManager> biddy = PolyBind.optionBinder(
                binder,
                Key.get(IntermediaryDataManager.class)
            );
            biddy.addBinding("local").to(LocalIntermediaryDataManager.class);
            biddy.addBinding("deepstore").to(DeepStorageIntermediaryDataManager.class).in(LazySingleton.class);
          }

          @Provides
          @LazySingleton
          @Named(ServiceStatusMonitor.HEARTBEAT_TAGS_BINDING)
          public Supplier<Map<String, Object>> heartbeatDimensions(WorkerConfig workerConfig, WorkerTaskManager workerTaskManager)
          {
            return () -> ImmutableMap.of(
                RobuxMetrics.WORKER_VERSION, workerConfig.getVersion(),
                RobuxMetrics.CATEGORY, workerConfig.getCategory(),
                RobuxMetrics.STATUS, workerTaskManager.isWorkerEnabled() ? "Enabled" : "Disabled"
            );
          }

          @Provides
          @LazySingleton
          public Worker getWorker(@Self RobuxNode node, WorkerConfig config)
          {
            return new Worker(
                node.getServiceScheme(),
                node.getHostAndPortToUse(),
                config.getIp(),
                config.getCapacity(),
                config.getVersion(),
                config.getCategory()
            );
          }

          @Provides
          @LazySingleton
          public WorkerNodeService getWorkerNodeService(WorkerConfig workerConfig)
          {
            return new WorkerNodeService(
                workerConfig.getIp(),
                workerConfig.getCapacity(),
                workerConfig.getVersion(),
                workerConfig.getCategory()
            );
          }
        },
        new ShuffleModule(),
        new IndexingServiceInputSourceModule(),
        new IndexingServiceTaskLogsModule(),
        new IndexingServiceTuningConfigModule(),
        new InputSourceModule(),
        new HadoopIndexTaskModule(),
        new LookupSerdeModule()
    );
  }

  public static Module makeWorkerManagementModule(boolean isZkEnabled)
  {
    return new Module()
    {
      @Override
      public void configure(Binder binder)
      {
        if (isZkEnabled) {
          binder.bind(WorkerTaskManager.class).to(WorkerTaskMonitor.class);
          binder.bind(WorkerTaskMonitor.class).in(ManageLifecycle.class);
          binder.bind(WorkerCuratorCoordinator.class).in(ManageLifecycle.class);
          LifecycleModule.register(binder, WorkerTaskMonitor.class);
        } else {
          binder.bind(WorkerTaskManager.class).in(ManageLifecycle.class);
        }

        Jerseys.addResource(binder, WorkerResource.class);
        Jerseys.addResource(binder, TaskManagementResource.class);
      }

      @Provides
      @ManageLifecycle
      public TaskStorageDirTracker getTaskStorageDirTracker(WorkerConfig workerConfig, TaskConfig taskConfig)
      {
        return TaskStorageDirTracker.fromConfigs(workerConfig, taskConfig);
      }
    };
  }
}

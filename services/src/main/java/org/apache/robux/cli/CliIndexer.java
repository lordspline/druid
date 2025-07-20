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
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.RobuxServerConfig;
import org.apache.robux.curator.ZkEnablementConfig;
import org.apache.robux.discovery.DataNodeService;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.discovery.WorkerNodeService;
import org.apache.robux.guice.RobuxProcessingModule;
import org.apache.robux.guice.IndexerServiceModule;
import org.apache.robux.guice.IndexingServiceInputSourceModule;
import org.apache.robux.guice.IndexingServiceModuleHelper;
import org.apache.robux.guice.IndexingServiceTaskLogsModule;
import org.apache.robux.guice.IndexingServiceTuningConfigModule;
import org.apache.robux.guice.Jerseys;
import org.apache.robux.guice.JoinableFactoryModule;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.guice.QueryRunnerFactoryModule;
import org.apache.robux.guice.QueryableModule;
import org.apache.robux.guice.QueryablePeonModule;
import org.apache.robux.guice.SegmentWranglerModule;
import org.apache.robux.guice.ServerTypeConfig;
import org.apache.robux.guice.annotations.AttemptId;
import org.apache.robux.guice.annotations.Parent;
import org.apache.robux.guice.annotations.RemoteChatHandler;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.indexer.HadoopIndexTaskModule;
import org.apache.robux.indexer.report.TaskReportFileWriter;
import org.apache.robux.indexing.common.MultipleFileTaskReportFileWriter;
import org.apache.robux.indexing.overlord.TaskRunner;
import org.apache.robux.indexing.overlord.ThreadingTaskRunner;
import org.apache.robux.indexing.worker.Worker;
import org.apache.robux.indexing.worker.WorkerTaskManager;
import org.apache.robux.indexing.worker.config.WorkerConfig;
import org.apache.robux.indexing.worker.shuffle.ShuffleModule;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.metadata.input.InputSourceModule;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.lookup.LookupModule;
import org.apache.robux.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.robux.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.ResponseContextConfig;
import org.apache.robux.server.SegmentManager;
import org.apache.robux.server.coordination.SegmentBootstrapper;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordination.ZkCoordinator;
import org.apache.robux.server.http.HistoricalResource;
import org.apache.robux.server.http.SegmentListerResource;
import org.apache.robux.server.http.SelfDiscoveryResource;
import org.apache.robux.server.initialization.jetty.CliIndexerServerModule;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.apache.robux.server.metrics.IndexerTaskCountStatsProvider;
import org.apache.robux.storage.local.LocalTmpStorageConfig;
import org.eclipse.jetty.server.Server;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 *
 */
@Command(
    name = "indexer",
    description = "Runs an Indexer. The Indexer is a task execution process that runs each task in a separate thread."
)
public class CliIndexer extends ServerRunnable
{
  private static final Logger log = new Logger(CliIndexer.class);

  private Properties properties;
  private boolean isZkEnabled = true;

  public CliIndexer()
  {
    super(log);
  }

  @Inject
  public void configure(Properties properties)
  {
    this.properties = properties;
    isZkEnabled = ZkEnablementConfig.isEnabled(properties);
  }

  @Override
  protected Set<NodeRole> getNodeRoles(Properties properties)
  {
    return ImmutableSet.of(NodeRole.INDEXER);
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
        new IndexerServiceModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            validateCentralizedDatasourceSchemaConfig(properties);

            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("robux/indexer");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(8091);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(8291);
            binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(true));
            // needed for the CliPeon, not needed for indexer, but have to bind annotation.
            binder.bindConstant().annotatedWith(AttemptId.class).to("");

            IndexingServiceModuleHelper.configureTaskRunnerConfigs(binder);

            JsonConfigProvider.bind(binder, "robux", RobuxNode.class, Parent.class);
            JsonConfigProvider.bind(binder, "robux.worker", WorkerConfig.class);

            CliPeon.configureIntermediaryData(binder);
            CliPeon.bindTaskConfigAndClients(binder);

            binder.bind(TaskReportFileWriter.class).toInstance(new MultipleFileTaskReportFileWriter());

            binder.bind(TaskRunner.class).to(ThreadingTaskRunner.class);
            binder.bind(QuerySegmentWalker.class).to(ThreadingTaskRunner.class);
            binder.bind(ThreadingTaskRunner.class).in(LazySingleton.class);
            binder.bind(IndexerTaskCountStatsProvider.class).to(WorkerTaskManager.class);

            CliPeon.bindRowIngestionMeters(binder);
            CliPeon.bindChatHandler(binder);
            CliPeon.bindPeonDataSegmentHandlers(binder);
            CliPeon.bindRealtimeCache(binder);
            CliPeon.bindCoordinatorHandoffNotifer(binder);
            binder.install(CliMiddleManager.makeWorkerManagementModule(isZkEnabled));

            binder.bind(AppenderatorsManager.class)
                  .to(UnifiedIndexerAppenderatorsManager.class)
                  .in(LazySingleton.class);

            binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.INDEXER_EXECUTOR));

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class);
            Jerseys.addResource(binder, SegmentListerResource.class);

            LifecycleModule.register(binder, Server.class, RemoteChatHandler.class);

            binder.bind(SegmentManager.class).in(LazySingleton.class);
            binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
            Jerseys.addResource(binder, HistoricalResource.class);

            if (isZkEnabled) {
              LifecycleModule.register(binder, ZkCoordinator.class);
            }
            LifecycleModule.register(binder, SegmentBootstrapper.class);

            bindAnnouncer(
                binder,
                DiscoverySideEffectsProvider.create()
            );

            Jerseys.addResource(binder, SelfDiscoveryResource.class);
            LifecycleModule.registerKey(binder, Key.get(SelfDiscoveryResource.class));
            binder.bind(LocalTmpStorageConfig.class)
                  .toProvider(new LocalTmpStorageConfig.DefaultLocalTmpStorageConfigProvider("indexer"))
                  .in(LazySingleton.class);
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

          @Provides
          @LazySingleton
          public DataNodeService getDataNodeService(RobuxServerConfig serverConfig)
          {
            return new DataNodeService(
                RobuxServer.DEFAULT_TIER,
                serverConfig.getMaxSize(),
                ServerType.INDEXER_EXECUTOR,
                RobuxServer.DEFAULT_PRIORITY
            );
          }
        },
        new ShuffleModule(),
        new IndexingServiceInputSourceModule(),
        new IndexingServiceTaskLogsModule(),
        new IndexingServiceTuningConfigModule(),
        new InputSourceModule(),
        new HadoopIndexTaskModule(),
        new QueryablePeonModule(),
        new CliIndexerServerModule(properties),
        new LookupModule()
    );
  }
}

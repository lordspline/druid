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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.netty.util.SuppressForbidden;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.curator.ZkEnablementConfig;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.Binders;
import org.apache.robux.guice.CacheModule;
import org.apache.robux.guice.IndexingServiceInputSourceModule;
import org.apache.robux.guice.IndexingServiceTaskLogsModule;
import org.apache.robux.guice.IndexingServiceTuningConfigModule;
import org.apache.robux.guice.Jerseys;
import org.apache.robux.guice.JoinableFactoryModule;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.guice.ManageLifecycleServer;
import org.apache.robux.guice.PeonProcessingModule;
import org.apache.robux.guice.PolyBind;
import org.apache.robux.guice.QueryRunnerFactoryModule;
import org.apache.robux.guice.QueryableModule;
import org.apache.robux.guice.QueryablePeonModule;
import org.apache.robux.guice.SegmentWranglerModule;
import org.apache.robux.guice.ServerTypeConfig;
import org.apache.robux.guice.annotations.AttemptId;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.guice.annotations.Parent;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.indexer.HadoopIndexTaskModule;
import org.apache.robux.indexer.report.SingleFileTaskReportFileWriter;
import org.apache.robux.indexer.report.TaskReportFileWriter;
import org.apache.robux.indexing.common.RetryPolicyConfig;
import org.apache.robux.indexing.common.RetryPolicyFactory;
import org.apache.robux.indexing.common.TaskToolboxFactory;
import org.apache.robux.indexing.common.actions.RemoteTaskActionClientFactory;
import org.apache.robux.indexing.common.actions.TaskActionClientFactory;
import org.apache.robux.indexing.common.config.TaskConfig;
import org.apache.robux.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.robux.indexing.common.task.Task;
import org.apache.robux.indexing.common.task.batch.parallel.DeepStorageShuffleClient;
import org.apache.robux.indexing.common.task.batch.parallel.HttpShuffleClient;
import org.apache.robux.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClientProvider;
import org.apache.robux.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClientProviderImpl;
import org.apache.robux.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.robux.indexing.overlord.SingleTaskBackgroundRunner;
import org.apache.robux.indexing.overlord.TaskRunner;
import org.apache.robux.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.robux.indexing.worker.executor.ExecutorLifecycle;
import org.apache.robux.indexing.worker.executor.ExecutorLifecycleConfig;
import org.apache.robux.indexing.worker.shuffle.DeepStorageIntermediaryDataManager;
import org.apache.robux.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.robux.indexing.worker.shuffle.LocalIntermediaryDataManager;
import org.apache.robux.java.util.common.lifecycle.Lifecycle;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.metadata.input.InputSourceModule;
import org.apache.robux.query.RobuxMetrics;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.lookup.LookupModule;
import org.apache.robux.segment.handoff.CoordinatorBasedSegmentHandoffNotifierConfig;
import org.apache.robux.segment.handoff.CoordinatorBasedSegmentHandoffNotifierFactory;
import org.apache.robux.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.robux.segment.incremental.RowIngestionMetersFactory;
import org.apache.robux.segment.loading.DataSegmentArchiver;
import org.apache.robux.segment.loading.DataSegmentKiller;
import org.apache.robux.segment.loading.DataSegmentMover;
import org.apache.robux.segment.loading.OmniDataSegmentArchiver;
import org.apache.robux.segment.loading.OmniDataSegmentKiller;
import org.apache.robux.segment.loading.OmniDataSegmentMover;
import org.apache.robux.segment.loading.StorageLocation;
import org.apache.robux.segment.realtime.ChatHandlerProvider;
import org.apache.robux.segment.realtime.NoopChatHandlerProvider;
import org.apache.robux.segment.realtime.ServiceAnnouncingChatHandlerProvider;
import org.apache.robux.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.robux.segment.realtime.appenderator.PeonAppenderatorsManager;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.ResponseContextConfig;
import org.apache.robux.server.SegmentManager;
import org.apache.robux.server.coordination.BroadcastDatasourceLoadingSpec;
import org.apache.robux.server.coordination.SegmentBootstrapper;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordination.ZkCoordinator;
import org.apache.robux.server.http.HistoricalResource;
import org.apache.robux.server.http.SegmentListerResource;
import org.apache.robux.server.initialization.jetty.ChatHandlerServerModule;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.apache.robux.server.lookup.cache.LookupLoadingSpec;
import org.apache.robux.server.metrics.DataSourceTaskIdHolder;
import org.apache.robux.server.metrics.ServiceStatusMonitor;
import org.apache.robux.storage.local.LocalTmpStorageConfig;
import org.apache.robux.tasklogs.TaskPayloadManager;
import org.eclipse.jetty.server.Server;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
@Command(
    name = "peon",
    description = "Runs a Peon, this is an individual forked \"task\" used as part of the indexing service. "
                  + "This should rarely, if ever, be used directly. "
                  + "See https://robux.apache.org/docs/latest/design/peons.html for a description"
)
public class CliPeon extends GuiceRunnable
{
  @SuppressWarnings("WeakerAccess")
  @Required
  @Arguments(description = "taskDirPath attemptId")
  public List<String> taskAndStatusFile;

  // path to the task Directory
  private String taskDirPath;

  // the attemptId
  private String attemptId;

  /**
   * Still using --nodeType as the flag for backward compatibility, although the concept is now more precisely called
   * "serverType".
   */
  @Option(name = "--nodeType", title = "nodeType", description = "Set the node type to expose on ZK")
  public String serverType = "indexer-executor";

  private boolean isZkEnabled = true;

  /**
   * <p> This option is deprecated, see {@link #loadBroadcastDatasourcesMode} option. </p>
   *
   * If set to "true", the peon will bind classes necessary for loading broadcast segments. This is used for
   * queryable tasks, such as streaming ingestion tasks.
   *
   */
  @Deprecated
  @Option(name = "--loadBroadcastSegments", title = "loadBroadcastSegments",
      description = "Enable loading of broadcast segments. This option is deprecated and will be removed in a"
                    + " future release. Use --loadBroadcastDatasourceMode instead.")
  public String loadBroadcastSegments = "false";

  /**
   * Broadcast datasource loading mode. The peon will bind classes necessary required for loading broadcast segments if
   * the mode is {@link BroadcastDatasourceLoadingSpec.Mode#ALL} or {@link BroadcastDatasourceLoadingSpec.Mode#ONLY_REQUIRED}.
   */
  @Option(name = "--loadBroadcastDatasourceMode", title = "loadBroadcastDatasourceMode",
      description = "Specify the broadcast datasource loading mode for the peon. Supported values are ALL, NONE, ONLY_REQUIRED.")
  public String loadBroadcastDatasourcesMode = BroadcastDatasourceLoadingSpec.Mode.ALL.toString();

  @Option(name = "--taskId", title = "taskId", description = "TaskId for fetching task.json remotely")
  public String taskId = "";

  private static final Logger log = new Logger(CliPeon.class);

  private Properties properties;

  public CliPeon()
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
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new PeonProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new SegmentWranglerModule(),
        new JoinableFactoryModule(),
        new IndexingServiceTaskLogsModule(),
        new Module()
        {
          @SuppressForbidden(reason = "System#out, System#err")
          @Override
          public void configure(Binder binder)
          {
            ServerRunnable.validateCentralizedDatasourceSchemaConfig(getProperties());
            Preconditions.checkArgument(
                taskAndStatusFile.size() >= 2,
                "taskAndStatusFile array should contain 2 or more elements. Current array elements: [%s]",
                taskAndStatusFile.toString()
            );

            taskDirPath = taskAndStatusFile.get(0);
            attemptId = taskAndStatusFile.get(1);

            binder.bindConstant().annotatedWith(Names.named("serviceName")).to("robux/peon");
            binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
            binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
            binder.bind(ResponseContextConfig.class).toInstance(ResponseContextConfig.newConfig(true));
            binder.bindConstant().annotatedWith(AttemptId.class).to(attemptId);

            JsonConfigProvider.bind(binder, "robux.task.executor", RobuxNode.class, Parent.class);

            bindRowIngestionMeters(binder);
            bindChatHandler(binder);
            configureIntermediaryData(binder);
            bindTaskConfigAndClients(binder);
            bindPeonDataSegmentHandlers(binder);

            binder.bind(ExecutorLifecycle.class).in(ManageLifecycle.class);
            LifecycleModule.register(binder, ExecutorLifecycle.class);
            ExecutorLifecycleConfig executorLifecycleConfig = new ExecutorLifecycleConfig()
                .setTaskFile(Paths.get(taskDirPath, "task.json").toFile())
                .setStatusFile(Paths.get(taskDirPath, "attempt", attemptId, "status.json").toFile());

            binder.bind(Properties.class).toInstance(properties);
            if (properties.getProperty("robux.indexer.runner.type", "").contains("k8s")) {
              log.info("Running peon in k8s mode");
              executorLifecycleConfig.setParentStreamDefined(false);
            }

            binder.bind(ExecutorLifecycleConfig.class).toInstance(executorLifecycleConfig);

            binder.bind(TaskReportFileWriter.class)
                  .toInstance(
                      new SingleFileTaskReportFileWriter(
                          Paths.get(taskDirPath, "attempt", attemptId, "report.json").toFile()
                      ));

            binder.bind(TaskRunner.class).to(SingleTaskBackgroundRunner.class);
            binder.bind(QuerySegmentWalker.class).to(SingleTaskBackgroundRunner.class);
            // Bind to ManageLifecycleServer to ensure SingleTaskBackgroundRunner is closed before
            // its dependent services, such as DiscoveryServiceLocator and OverlordClient.
            // This order ensures that tasks can finalize their cleanup operations before service location closure.
            binder.bind(SingleTaskBackgroundRunner.class).in(ManageLifecycleServer.class);

            bindRealtimeCache(binder);
            bindCoordinatorHandoffNotifer(binder);

            binder.bind(AppenderatorsManager.class)
                  .to(PeonAppenderatorsManager.class)
                  .in(LazySingleton.class);

            binder.bind(JettyServerInitializer.class).to(QueryJettyServerInitializer.class);
            Jerseys.addResource(binder, SegmentListerResource.class);
            binder.bind(ServerTypeConfig.class).toInstance(new ServerTypeConfig(ServerType.fromString(serverType)));
            LifecycleModule.register(binder, Server.class);

            final BroadcastDatasourceLoadingSpec.Mode mode =
                BroadcastDatasourceLoadingSpec.Mode.valueOf(loadBroadcastDatasourcesMode);
            if ("true".equals(loadBroadcastSegments)
                || mode == BroadcastDatasourceLoadingSpec.Mode.ALL
                || mode == BroadcastDatasourceLoadingSpec.Mode.ONLY_REQUIRED) {
              binder.install(new BroadcastSegmentLoadingModule());
            }
          }

          @Provides
          @LazySingleton
          @Named(ServiceStatusMonitor.HEARTBEAT_TAGS_BINDING)
          public Supplier<Map<String, Object>> heartbeatDimensions(Task task)
          {
            return () -> CliPeon.heartbeatDimensions(task);
          }

          @Provides
          @LazySingleton
          public Task readTask(@Json ObjectMapper mapper, ExecutorLifecycleConfig config, TaskPayloadManager taskPayloadManager)
          {
            try {
              if (!config.getTaskFile().exists() || config.getTaskFile().length() == 0) {
                log.info("Task file not found, trying to pull task payload from deep storage");
                String task = IOUtils.toString(taskPayloadManager.streamTaskPayload(taskId).get(), Charset.defaultCharset());
                // write the remote task.json to the task file location for ExecutorLifecycle to pickup
                FileUtils.write(config.getTaskFile(), task, Charset.defaultCharset());
              }
              return mapper.readValue(config.getTaskFile(), Task.class);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Provides
          @LazySingleton
          @Named(DataSourceTaskIdHolder.DATA_SOURCE_BINDING)
          public String getDataSourceFromTask(final Task task)
          {
            return task.getDataSource();
          }

          @Provides
          @LazySingleton
          @Named(DataSourceTaskIdHolder.TASK_ID_BINDING)
          public String getTaskIDFromTask(final Task task)
          {
            return task.getId();
          }

          @Provides
          @LazySingleton
          @Named(DataSourceTaskIdHolder.LOOKUPS_TO_LOAD_FOR_TASK)
          public LookupLoadingSpec getLookupsToLoad(final Task task)
          {
            return task.getLookupLoadingSpec();
          }

          @Provides
          @LazySingleton
          @Named(DataSourceTaskIdHolder.BROADCAST_DATASOURCES_TO_LOAD_FOR_TASK)
          public BroadcastDatasourceLoadingSpec getBroadcastDatasourcesToLoad(final Task task)
          {
            return task.getBroadcastDatasourceLoadingSpec();
          }

          @Provides
          @LazySingleton
          public LocalTmpStorageConfig getLocalTmpStorage()
          {
            File tmpDir = new File(taskDirPath, "tmp");
            try {
              org.apache.robux.java.util.common.FileUtils.mkdirp(tmpDir);
            }
            catch (IOException e) {
              log.error("Failed to create tmp directory for the task");
              throw new RuntimeException(e);
            }
            return () -> tmpDir;
          }
        },
        new QueryablePeonModule(),
        new IndexingServiceInputSourceModule(),
        new IndexingServiceTuningConfigModule(),
        new InputSourceModule(),
        new HadoopIndexTaskModule(),
        new ChatHandlerServerModule(properties),
        new LookupModule()
    );
  }

  @SuppressForbidden(reason = "System#out, System#err")
  @Override
  public void run()
  {
    try {
      Injector injector = makeInjector(ImmutableSet.of(NodeRole.PEON));
      try {
        final Lifecycle lifecycle = initLifecycle(injector);
        final Thread hook = new Thread(
            () -> {
              log.info("Running shutdown hook");
              lifecycle.stop();
            }
        );
        Runtime.getRuntime().addShutdownHook(hook);
        injector.getInstance(ExecutorLifecycle.class).join();

        // Sanity check to help debug unexpected non-daemon threads
        final Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread thread : threadSet) {
          if (!thread.isDaemon() && thread != Thread.currentThread()) {
            log.info("Thread [%s] is non daemon.", thread);
          }
        }

        // Explicitly call lifecycle stop, dont rely on shutdown hook.
        lifecycle.stop();
        try {
          Runtime.getRuntime().removeShutdownHook(hook);
        }
        catch (IllegalStateException e) {
          System.err.println("Cannot remove shutdown hook, already shutting down!");
        }
      }
      catch (Throwable t) {
        System.err.println("Error!");
        System.err.println(Throwables.getStackTraceAsString(t));
        System.exit(1);
      }
      System.out.println("Finished peon task");
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void bindRowIngestionMeters(Binder binder)
  {
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
  }

  static void bindChatHandler(Binder binder)
  {
    PolyBind.createChoice(
        binder,
        "robux.indexer.task.chathandler.type",
        Key.get(ChatHandlerProvider.class),
        Key.get(ServiceAnnouncingChatHandlerProvider.class)
    );
    final MapBinder<String, ChatHandlerProvider> handlerProviderBinder =
        PolyBind.optionBinder(binder, Key.get(ChatHandlerProvider.class));
    handlerProviderBinder
        .addBinding("announce")
        .to(ServiceAnnouncingChatHandlerProvider.class)
        .in(LazySingleton.class);
    handlerProviderBinder
        .addBinding("noop")
        .to(NoopChatHandlerProvider.class)
        .in(LazySingleton.class);
    binder.bind(ServiceAnnouncingChatHandlerProvider.class).in(LazySingleton.class);
    binder.bind(NoopChatHandlerProvider.class).in(LazySingleton.class);
  }

  static void bindPeonDataSegmentHandlers(Binder binder)
  {
    // Build it to make it bind even if nothing binds to it.
    bindDataSegmentKiller(binder);
    Binders.dataSegmentMoverBinder(binder);
    binder.bind(DataSegmentMover.class).to(OmniDataSegmentMover.class).in(LazySingleton.class);
    Binders.dataSegmentArchiverBinder(binder);
    binder.bind(DataSegmentArchiver.class).to(OmniDataSegmentArchiver.class).in(LazySingleton.class);
  }

  static void bindDataSegmentKiller(Binder binder)
  {
    Binders.dataSegmentKillerBinder(binder);
    binder.bind(DataSegmentKiller.class).to(OmniDataSegmentKiller.class).in(LazySingleton.class);
  }

  private static void configureTaskActionClient(Binder binder)
  {
    binder.bind(TaskActionClientFactory.class)
          .to(RemoteTaskActionClientFactory.class)
          .in(LazySingleton.class);

    binder.bind(NodeRole.class).annotatedWith(Self.class).toInstance(NodeRole.PEON);
  }

  static void bindTaskConfigAndClients(Binder binder)
  {
    binder.bind(TaskToolboxFactory.class).in(LazySingleton.class);

    JsonConfigProvider.bind(binder, "robux.indexer.task", TaskConfig.class);
    JsonConfigProvider.bind(binder, "robux.peon.taskActionClient.retry", RetryPolicyConfig.class);

    configureTaskActionClient(binder);

    binder.bind(ParallelIndexSupervisorTaskClientProvider.class)
          .to(ParallelIndexSupervisorTaskClientProviderImpl.class)
          .in(LazySingleton.class);

    binder.bind(RetryPolicyFactory.class).in(LazySingleton.class);
  }

  static void bindRealtimeCache(Binder binder)
  {
    JsonConfigProvider.bind(binder, "robux.realtime.cache", CacheConfig.class);
    binder.install(new CacheModule());
  }

  static void bindCoordinatorHandoffNotifer(Binder binder)
  {
    JsonConfigProvider.bind(
        binder,
        "robux.segment.handoff",
        CoordinatorBasedSegmentHandoffNotifierConfig.class
    );
    binder.bind(SegmentHandoffNotifierFactory.class)
          .to(CoordinatorBasedSegmentHandoffNotifierFactory.class)
          .in(LazySingleton.class);
  }

  static void configureIntermediaryData(Binder binder)
  {
    PolyBind.createChoice(
        binder,
        "robux.processing.intermediaryData.storage.type",
        Key.get(IntermediaryDataManager.class),
        Key.get(LocalIntermediaryDataManager.class)
    );
    final MapBinder<String, IntermediaryDataManager> intermediaryDataManagerBiddy = PolyBind.optionBinder(
        binder,
        Key.get(IntermediaryDataManager.class)
    );
    intermediaryDataManagerBiddy.addBinding("local").to(LocalIntermediaryDataManager.class).in(LazySingleton.class);
    intermediaryDataManagerBiddy.addBinding("deepstore").to(DeepStorageIntermediaryDataManager.class).in(LazySingleton.class);

    PolyBind.createChoice(
        binder,
        "robux.processing.intermediaryData.storage.type",
        Key.get(ShuffleClient.class),
        Key.get(HttpShuffleClient.class)
    );
    final MapBinder<String, ShuffleClient> shuffleClientBiddy = PolyBind.optionBinder(
        binder,
        Key.get(ShuffleClient.class)
    );
    shuffleClientBiddy.addBinding("local").to(HttpShuffleClient.class).in(LazySingleton.class);
    shuffleClientBiddy.addBinding("deepstore").to(DeepStorageShuffleClient.class).in(LazySingleton.class);
  }

  static Map<String, Object> heartbeatDimensions(Task task)
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put(RobuxMetrics.TASK_ID, task.getId());
    builder.put(RobuxMetrics.DATASOURCE, task.getDataSource());
    builder.put(RobuxMetrics.TASK_TYPE, task.getType());
    builder.put(RobuxMetrics.GROUP_ID, task.getGroupId());
    Map<String, Object> tags = task.getContextValue(RobuxMetrics.TAGS);
    if (tags != null && !tags.isEmpty()) {
      builder.put(RobuxMetrics.TAGS, tags);
    }

    if (task instanceof SeekableStreamIndexTask) {
      SeekableStreamIndexTask streamingTask = (SeekableStreamIndexTask) task;
      String status = streamingTask.getCurrentRunnerStatus();
      if (status != null) {
        builder.put(RobuxMetrics.STATUS, status);
      }
    }

    return builder.build();
  }

  public class BroadcastSegmentLoadingModule implements Module
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bind(SegmentManager.class).in(LazySingleton.class);
      binder.bind(ZkCoordinator.class).in(ManageLifecycle.class);
      Jerseys.addResource(binder, HistoricalResource.class);

      if (isZkEnabled) {
        LifecycleModule.register(binder, ZkCoordinator.class);
      }
      LifecycleModule.register(binder, SegmentBootstrapper.class);
    }

    @Provides
    @LazySingleton
    public List<StorageLocation> getCliPeonStorageLocations(TaskConfig config)
    {
      File broadcastStorage = new File(new File(taskDirPath, "broadcast"), "segments");

      return ImmutableList.of(new StorageLocation(broadcastStorage, config.getTmpStorageBytesPerTask(), null));
    }
  }
}

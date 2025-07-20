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

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.indexer.HadoopRobuxDetermineConfigurationJob;
import org.apache.robux.indexer.HadoopRobuxIndexerConfig;
import org.apache.robux.indexer.HadoopRobuxIndexerJob;
import org.apache.robux.indexer.HadoopIngestionSpec;
import org.apache.robux.indexer.JobHelper;
import org.apache.robux.indexer.Jobby;
import org.apache.robux.indexer.path.MetadataStoreBasedUsedSegmentsRetriever;
import org.apache.robux.indexer.path.SegmentMetadataPublisher;
import org.apache.robux.indexer.updater.MetadataStorageUpdaterJobSpec;
import org.apache.robux.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.robux.metadata.MetadataStorageConnectorConfig;
import org.apache.robux.metadata.MetadataStorageTablesConfig;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 */
@Command(
    name = "hadoop-indexer",
    description = "Runs the batch Hadoop Robux Indexer, see https://robux.apache.org/docs/latest/Batch-ingestion.html for a description."
)
public class CliInternalHadoopIndexer extends GuiceRunnable
{
  private static final Logger log = new Logger(CliHadoopIndexer.class);

  @Arguments(description = "A JSON object or the path to a file that contains a JSON object")
  @Required
  private String argumentSpec;

  private HadoopRobuxIndexerConfig config;

  public CliInternalHadoopIndexer()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        binder -> {
          binder.bindConstant().annotatedWith(Names.named("serviceName")).to("robux/internal-hadoop-indexer");
          binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
          binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);

          // bind metadata storage config based on HadoopIOConfig
          MetadataStorageUpdaterJobSpec metadataSpec = getHadoopRobuxIndexerConfig().getSchema()
                                                                                    .getIOConfig()
                                                                                    .getMetadataUpdateSpec();

          binder.bind(new TypeLiteral<Supplier<MetadataStorageConnectorConfig>>() {})
                .toInstance(metadataSpec);
          binder.bind(MetadataStorageTablesConfig.class).toInstance(metadataSpec.getMetadataStorageTablesConfig());
          binder.bind(IndexerMetadataStorageCoordinator.class).to(IndexerSQLMetadataStorageCoordinator.class).in(
              LazySingleton.class
          );
        }
    );
  }

  @Override
  public void run()
  {
    try {
      Injector injector = makeInjector();

      config = getHadoopRobuxIndexerConfig();

      MetadataStorageUpdaterJobSpec metadataSpec = config.getSchema().getIOConfig().getMetadataUpdateSpec();
      // override metadata storage type based on HadoopIOConfig
      Preconditions.checkNotNull(metadataSpec.getType(), "type in metadataUpdateSpec must not be null");
      injector.getInstance(Properties.class).setProperty("robux.metadata.storage.type", metadataSpec.getType());

      final IndexerMetadataStorageCoordinator storageCoordinator
          = injector.getInstance(IndexerMetadataStorageCoordinator.class);
      HadoopIngestionSpec.updateSegmentListIfDatasourcePathSpecIsUsed(
          config.getSchema(),
          HadoopRobuxIndexerConfig.JSON_MAPPER,
          new MetadataStoreBasedUsedSegmentsRetriever(storageCoordinator)
      );

      List<Jobby> jobs = new ArrayList<>();
      HadoopRobuxIndexerJob indexerJob = new HadoopRobuxIndexerJob(
          config,
          new SegmentMetadataPublisher(storageCoordinator)
      );
      jobs.add(new HadoopRobuxDetermineConfigurationJob(config));
      jobs.add(indexerJob);
      boolean jobsSucceeded = JobHelper.runJobs(jobs);
      JobHelper.renameIndexFilesForSegments(config.getSchema(), indexerJob.getPublishedSegmentAndIndexZipFilePaths());
      JobHelper.maybeDeleteIntermediatePath(jobsSucceeded, config.getSchema());

    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public HadoopRobuxIndexerConfig getHadoopRobuxIndexerConfig()
  {
    if (config == null) {
      try {
        if (argumentSpec.startsWith("{")) {
          config = HadoopRobuxIndexerConfig.fromString(argumentSpec);
        } else {
          File localConfigFile = null;

          try {
            final URI argumentSpecUri = new URI(argumentSpec);
            final String argumentSpecScheme = argumentSpecUri.getScheme();

            if (argumentSpecScheme == null || "file".equals(argumentSpecScheme)) {
              // File URI.
              localConfigFile = new File(argumentSpecUri.getPath());
            }
          }
          catch (URISyntaxException e) {
            // Not a URI, assume it's a local file.
            localConfigFile = new File(argumentSpec);
          }

          if (localConfigFile != null) {
            config = HadoopRobuxIndexerConfig.fromFile(localConfigFile);
          } else {
            config = HadoopRobuxIndexerConfig.fromDistributedFileSystem(argumentSpec);
          }
        }
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return config;
  }
}

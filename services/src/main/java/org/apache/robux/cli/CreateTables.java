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
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.robux.guice.RobuxProcessingModule;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.QueryRunnerFactoryModule;
import org.apache.robux.guice.QueryableModule;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.metadata.MetadataStorageConnector;
import org.apache.robux.metadata.MetadataStorageConnectorConfig;
import org.apache.robux.metadata.MetadataStorageTablesConfig;
import org.apache.robux.server.RobuxNode;

import java.util.List;

@Command(
    name = "metadata-init",
    description = "Initialize Metadata Storage"
)
public class CreateTables extends GuiceRunnable
{
  @Option(name = "--connectURI", description = "Database JDBC connection string")
  @Required
  private String connectURI;

  @Option(name = "--user", description = "Database username")
  @Required
  private String user;

  @Option(name = "--password", description = "Database password")
  @Required
  private String password;

  @Option(name = "--base", description = "Base table name")
  private String base;

  private static final Logger log = new Logger(CreateTables.class);

  public CreateTables()
  {
    super(log);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        // It's unknown why those modules are required in CreateTables, and if all of those modules are required or not.
        // Maybe some of those modules could be removed.
        // See https://github.com/apache/robux/pull/4429#discussion_r123602930
        new RobuxProcessingModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        binder -> {
          JsonConfigProvider.bindInstance(
              binder,
              Key.get(MetadataStorageConnectorConfig.class),
              new MetadataStorageConnectorConfig()
              {
                @Override
                public String getConnectURI()
                {
                  return connectURI;
                }

                @Override
                public String getUser()
                {
                  return user;
                }

                @Override
                public String getPassword()
                {
                  return password;
                }
              }
          );
          JsonConfigProvider.bindInstance(
              binder,
              Key.get(MetadataStorageTablesConfig.class),
              MetadataStorageTablesConfig.fromBase(base)
          );
          JsonConfigProvider.bindInstance(
              binder,
              Key.get(RobuxNode.class, Self.class),
              new RobuxNode("tools", "localhost", false, -1, null, true, false)
          );
        }
    );
  }

  @Override
  public void run()
  {
    final Injector injector = makeInjector();
    MetadataStorageConnector dbConnector = injector.getInstance(MetadataStorageConnector.class);
    dbConnector.createDataSourceTable();
    dbConnector.createPendingSegmentsTable();
    dbConnector.createSegmentSchemasTable();
    dbConnector.createSegmentTable();
    dbConnector.createUpgradeSegmentsTable();
    dbConnector.createRulesTable();
    dbConnector.createConfigTable();
    dbConnector.createTaskTables();
    dbConnector.createAuditTable();
    dbConnector.createSupervisorsTable();
  }
}

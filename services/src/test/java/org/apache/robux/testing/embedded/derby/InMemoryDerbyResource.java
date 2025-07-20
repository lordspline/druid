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

package org.apache.robux.testing.embedded.derby;

import org.apache.robux.metadata.TestDerbyConnector;
import org.apache.robux.testing.embedded.EmbeddedRobuxCluster;
import org.apache.robux.testing.embedded.EmbeddedResource;

/**
 * Resource to run an embedded Derby metadata store in memory.
 */
public class InMemoryDerbyResource implements EmbeddedResource
{
  private final TestDerbyConnector.DerbyConnectorRule dbRule;

  public InMemoryDerbyResource()
  {
    this.dbRule = new TestDerbyConnector.DerbyConnectorRule();
  }

  @Override
  public void start()
  {
    dbRule.before();
  }

  @Override
  public void stop()
  {
    dbRule.after();
  }

  @Override
  public void onStarted(EmbeddedRobuxCluster cluster)
  {
    final TestDerbyConnector connector = dbRule.getConnector();
    cluster.addCommonProperty("robux.metadata.storage.type", InMemoryDerbyModule.TYPE);
    cluster.addCommonProperty("robux.metadata.storage.tables.base", connector.getMetadataTablesConfig().getBase());
    cluster.addCommonProperty("robux.metadata.storage.connector.connectURI", connector.getJdbcUri());
  }

  @Override
  public String toString()
  {
    return "InMemoryDerbyResource";
  }
}

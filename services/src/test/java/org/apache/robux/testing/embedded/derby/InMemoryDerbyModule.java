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

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.PolyBind;
import org.apache.robux.guice.SQLMetadataStorageRobuxModule;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.robux.metadata.MetadataStorageActionHandlerFactory;
import org.apache.robux.metadata.MetadataStorageConnector;
import org.apache.robux.metadata.MetadataStorageConnectorConfig;
import org.apache.robux.metadata.MetadataStorageProvider;
import org.apache.robux.metadata.MetadataStorageTablesConfig;
import org.apache.robux.metadata.SQLMetadataConnector;
import org.apache.robux.metadata.TestDerbyConnector;
import org.apache.robux.metadata.storage.derby.DerbyMetadataStorageProvider;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;

import java.util.Properties;

/**
 * Guice module to bind {@link SQLMetadataConnector} to {@link TestDerbyConnector}.
 * Used in Coordinator and Overlord embedded tests to connect to an in-memory Derby
 * database.
 * <p>
 * This module can be used as an extension in embedded tests thanks to the file
 * {@code services/src/test/resources/META-INF/services/org.apache.robux.initialization.RobuxModule}.
 */
public class InMemoryDerbyModule extends SQLMetadataStorageRobuxModule implements RobuxModule
{
  public static final String TYPE = "derbyInMemory";

  private Properties properties;

  public InMemoryDerbyModule()
  {
    super(TYPE);
  }

  @Inject
  public void setProperties(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    final String connectURI = properties.getProperty("robux.metadata.storage.connector.connectURI");
    final TestDerbyConnector connector = new TestDerbyConnector(
        MetadataStorageConnectorConfig.create(connectURI, null, null, null),
        MetadataStorageTablesConfig.fromBase(properties.getProperty("robux.metadata.storage.tables.base")),
        connectURI,
        CentralizedDatasourceSchemaConfig.create()
    );

    PolyBind.optionBinder(binder, Key.get(MetadataStorageProvider.class))
            .addBinding(TYPE)
            .to(DerbyMetadataStorageProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageConnector.class))
            .addBinding(TYPE)
            .toInstance(connector);

    PolyBind.optionBinder(binder, Key.get(SQLMetadataConnector.class))
            .addBinding(TYPE)
            .toInstance(connector);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
            .addBinding(TYPE)
            .to(DerbyMetadataStorageActionHandlerFactory.class)
            .in(LazySingleton.class);
  }
}

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

package org.apache.robux.metadata.storage.mysql;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.PolyBind;
import org.apache.robux.guice.SQLMetadataStorageRobuxModule;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.metadata.MetadataStorageActionHandlerFactory;
import org.apache.robux.metadata.MetadataStorageConnector;
import org.apache.robux.metadata.MetadataStorageProvider;
import org.apache.robux.metadata.MySQLMetadataStorageActionHandlerFactory;
import org.apache.robux.metadata.NoopMetadataStorageProvider;
import org.apache.robux.metadata.SQLMetadataConnector;
import org.apache.robux.metadata.input.MySQLInputSourceDatabaseConnector;

import java.util.Collections;
import java.util.List;

public class MySQLMetadataStorageModule extends SQLMetadataStorageRobuxModule implements RobuxModule
{
  public static final String TYPE = "mysql";

  public MySQLMetadataStorageModule()
  {
    super(TYPE);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule()
            .registerSubtypes(
                new NamedType(MySQLInputSourceDatabaseConnector.class, "mysql")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    JsonConfigProvider.bind(binder, "robux.metadata.mysql.ssl", MySQLConnectorSslConfig.class);
    JsonConfigProvider.bind(binder, "robux.metadata.mysql.driver", MySQLConnectorDriverConfig.class);

    PolyBind
        .optionBinder(binder, Key.get(MetadataStorageProvider.class))
        .addBinding(TYPE)
        .to(NoopMetadataStorageProvider.class)
        .in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(MetadataStorageConnector.class))
        .addBinding(TYPE)
        .to(MySQLConnector.class)
        .in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(SQLMetadataConnector.class))
        .addBinding(TYPE)
        .to(MySQLConnector.class)
        .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
            .addBinding(TYPE)
            .to(MySQLMetadataStorageActionHandlerFactory.class)
            .in(LazySingleton.class);

  }
}

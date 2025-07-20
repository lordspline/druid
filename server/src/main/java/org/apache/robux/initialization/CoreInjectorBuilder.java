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

package org.apache.robux.initialization;

import com.google.inject.Injector;
import org.apache.robux.curator.CuratorModule;
import org.apache.robux.curator.discovery.DiscoveryModule;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.AnnouncerModule;
import org.apache.robux.guice.BuiltInTypesModule;
import org.apache.robux.guice.CatalogCoreModule;
import org.apache.robux.guice.CoordinatorDiscoveryModule;
import org.apache.robux.guice.RobuxInjectorBuilder;
import org.apache.robux.guice.RobuxSecondaryModule;
import org.apache.robux.guice.ExpressionModule;
import org.apache.robux.guice.ExtensionsModule;
import org.apache.robux.guice.JacksonConfigManagerModule;
import org.apache.robux.guice.JavaScriptModule;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.LocalDataStorageRobuxModule;
import org.apache.robux.guice.MetadataConfigModule;
import org.apache.robux.guice.ServerModule;
import org.apache.robux.guice.ServerViewModule;
import org.apache.robux.guice.StartupLoggingModule;
import org.apache.robux.guice.StorageNodeModule;
import org.apache.robux.guice.annotations.Client;
import org.apache.robux.guice.annotations.EscalatedClient;
import org.apache.robux.guice.http.HttpClientModule;
import org.apache.robux.guice.security.AuthenticatorModule;
import org.apache.robux.guice.security.AuthorizerModule;
import org.apache.robux.guice.security.RobuxAuthModule;
import org.apache.robux.guice.security.EscalatorModule;
import org.apache.robux.guice.security.PolicyModule;
import org.apache.robux.metadata.storage.derby.DerbyMetadataStorageRobuxModule;
import org.apache.robux.rpc.guice.ServiceClientModule;
import org.apache.robux.segment.writeout.SegmentWriteOutMediumModule;
import org.apache.robux.server.emitter.EmitterModule;
import org.apache.robux.server.initialization.AuthenticatorMapperModule;
import org.apache.robux.server.initialization.AuthorizerMapperModule;
import org.apache.robux.server.initialization.ExternalStorageAccessSecurityModule;
import org.apache.robux.server.initialization.jetty.JettyServerModule;
import org.apache.robux.server.metrics.MetricsModule;
import org.apache.robux.server.security.TLSCertificateCheckerModule;
import org.apache.robux.storage.StorageConnectorModule;

import java.util.Collections;
import java.util.Set;

/**
 * Builds the core (common) set of modules used by all Robux services and
 * commands. The basic injector just adds logging and the Robux lifecycle.
 * Call {@link #forServer()} to add the server-specific modules.
 */
public class CoreInjectorBuilder extends RobuxInjectorBuilder
{
  public CoreInjectorBuilder(final Injector baseInjector)
  {
    this(baseInjector, Collections.emptySet());
  }

  public CoreInjectorBuilder(final Injector baseInjector, final Set<NodeRole> nodeRoles)
  {
    super(baseInjector, nodeRoles);
    add(RobuxSecondaryModule.class);
  }

  public CoreInjectorBuilder withLogging()
  {
    // New modules should be added after Log4jShutterDownerModule
    add(new Log4jShutterDownerModule());
    return this;
  }

  public CoreInjectorBuilder withLifecycle()
  {
    add(new LifecycleModule());
    return this;
  }

  public CoreInjectorBuilder forServer()
  {
    withLogging();
    withLifecycle();
    add(
        ExtensionsModule.SecondaryModule.class,
        new RobuxAuthModule(),
        new PolicyModule(),
        TLSCertificateCheckerModule.class,
        EmitterModule.class,
        HttpClientModule.global(),
        HttpClientModule.escalatedGlobal(),
        new HttpClientModule("robux.broker.http", Client.class, true),
        new HttpClientModule("robux.broker.http", EscalatedClient.class, true),
        new CuratorModule(),
        new AnnouncerModule(),
        new MetricsModule(),
        new SegmentWriteOutMediumModule(),
        new ServerModule(),
        new StorageNodeModule(),
        new JettyServerModule(),
        new ExpressionModule(),
        new BuiltInTypesModule(),
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
        new AuthenticatorMapperModule(),
        new EscalatorModule(),
        new AuthorizerModule(),
        new AuthorizerMapperModule(),
        new StartupLoggingModule(),
        new ExternalStorageAccessSecurityModule(),
        new ServiceClientModule(),
        new StorageConnectorModule(),
        new CatalogCoreModule()
    );
    return this;
  }
}
